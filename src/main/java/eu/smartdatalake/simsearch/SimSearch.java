package eu.smartdatalake.simsearch;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TObjectIntMap;

import eu.smartdatalake.simsearch.numerical.BPlusTree;
import eu.smartdatalake.simsearch.numerical.NumericalSimSearch;
import eu.smartdatalake.simsearch.categorical.CategoricalSimSearch;
import eu.smartdatalake.simsearch.categorical.CollectionTransformer;
import eu.smartdatalake.simsearch.categorical.IntSetCollection;
import eu.smartdatalake.simsearch.categorical.TokenSetCollection;

public class SimSearch implements Runnable {

	//FIXME: Check which types of queries should apply
	public final static int TYPE_CATEGORICAL_TOPK = 0;
	public final static int TYPE_SPATIAL_KNN = 1;   //TODO: not yet implemented
	public final static int TYPE_NUMERICAL_TOPK = 2;
	
	PrintStream logStream = null;
	
	int type;       //Type of the search query
	
	Index<Object, Object> index;    //Handle to the underlying index structure (e.g., B+-tree)
	TIntList[] idx;                 //Handle to the underlying inverted index (for categorical keyword search)
	
	long numResults;

	double searchingKey;
	public int collectionSize;
	double simThreshold;
	ConcurrentLinkedQueue<Result> resultsQueue;

	//Used in categorical similarity search only	
	IntSetCollection transformedTargetCollection;
	IntSetCollection transformedQueryCollection;

	// Handle to the similarity measure to be used
	ISimilarity<?> simMeasure;
	
	public AtomicBoolean running = new AtomicBoolean(false);
	
	public String name;    //An informative name given to the running instance of similarity search 

	/**
	 * Constructor for CATEGORICAL similarity search against against an already constructed index over sets of tokens.
	 * @param type  The type of the similarity search query (0: CATEGORICAL).
	 * @param name  A user-specified name given to the running instance of similarity search.
	 * @param targetCollection  The data collection with tokens to search for similarity.
	 * @param queryCollection  The query collection of tokens to search for similarity.
	 * @param collectionSize  The count of results to fetch.
	 * @param simMeasure  The similarity measure to be used in the search.
	 * @param resultsQueue  Queue to collect query results
	 * @param logStream  Output stream for logging messages and statistics
	 */
	public SimSearch(int type, String name, TokenSetCollection targetCollection, TokenSetCollection queryCollection, int collectionSize, ISimilarity simMeasure, ConcurrentLinkedQueue<Result> resultsQueue, PrintStream logStream) {

		super();
		this.logStream = logStream;
		this.type = type;
		this.name = name;
		this.simMeasure = simMeasure;
				
		// Transform the input collections
		long duration = System.nanoTime();
		CollectionTransformer transformer = new CollectionTransformer();
		// Create a global collection of tokens for the dictionary
		TokenSetCollection totalCollection = new TokenSetCollection();		
		totalCollection.sets.putAll(targetCollection.sets);
		totalCollection.sets.putAll(queryCollection.sets);
		// Dictionary must be common for the target and query set collections
		TObjectIntMap<String> tokenDictionary = transformer.constructTokenDictionary(totalCollection);
		this.transformedTargetCollection = transformer.transformCollection(targetCollection, tokenDictionary);
		this.transformedQueryCollection = transformer.transformCollection(queryCollection, tokenDictionary);
		duration = System.nanoTime() - duration;
		this.logStream.println("Transform time: " + duration / 1000000000.0 + " sec.");
		
		// Inverted index initialization
		duration = System.nanoTime();
		idx = new TIntList[this.transformedTargetCollection.numTokens];
		for (int i = 0; i < idx.length; i++) {
			idx[i] = new TIntArrayList();
		}

		// Inverted index construction
		for (int i = 0; i < this.transformedTargetCollection.sets.length; i++) {
			// Since no threshold is known beforehand, index is constructed with
			// all tokens (not prefixes)
			for (int j = 0; j < this.transformedTargetCollection.sets[i].length; j++) {
				idx[this.transformedTargetCollection.sets[i][j]].add(i);
			}
		}
		duration = System.nanoTime() - duration;
		this.logStream.println("Inverted index time: " + duration / 1000000000.0 + " sec.");
		
		this.collectionSize = collectionSize;
		this.index = null;
		this.resultsQueue = resultsQueue;
		this.numResults = 0;
	}
	
	/**
	 * Constructor for NUMERICAL similarity search queries against an already constructed index over numerical values.
	 * @param type  The type of the similarity search query (2: NUMERICAL).
	 * @param name  A user-specified name given to the running instance of similarity search.
	 * @param idx   The underlying index.
	 * @param searchingKey    The key value to search against the index.
	 * @param collectionSize  The count of results to fetch.
	 * @param simMeasure  The similarity measure to be used in the search.
	 * @param resultsQueue   Queue to collect query results
	 * @param logStream  Output stream for logging messages and statistics
	 */
	public SimSearch(int type, String name, Index<Object, Object> idx, double searchingKey, int collectionSize, ISimilarity simMeasure, ConcurrentLinkedQueue<Result> resultsQueue, PrintStream logStream) {

		super();
		this.logStream = logStream;
		this.type = type;
		this.name = name;
		this.collectionSize = collectionSize;
		this.searchingKey = searchingKey;
		this.index = idx;
		this.resultsQueue = resultsQueue;
		this.numResults = 0;
		this.simMeasure = simMeasure;
	}

	
	/**
	 * Instantiates a numerical similarity search query.
	 * @param idx  The B+-tree index used in the search. It contains (numerical) keys and (string) values for the object identifiers.
	 * @param searchingKey  The search key against the index (i.e., the numerical value specified by the query).
	 * @param results  The queue to collect query results.
	 */
	public void applyNumericalSimSearch(Index<Object, Object> idx, Double searchingKey, ConcurrentLinkedQueue<Result> results) {
		
		NumericalSimSearch<Double, String> numSearch =  new NumericalSimSearch<Double, String>((BPlusTree<Double, String>)idx, searchingKey, (ISimilarity<Double>) this.simMeasure, this.logStream);
		
		//Progressive fetching of results
		try {
			List<String> qryAnswer;
			double score;
			Result res;

			running.set(true);

			// Continue fetching results until process gets suspended
			while (running.get()) {  
			// ALTERNATIVE OPTION: until top-k results are retrieved
//			for (int i = 0; i < collectionSize; i++) { 

				if (running.get() == false) {
					break;
				}

				TimeUnit.NANOSECONDS.sleep(100);
				qryAnswer = numSearch.getNextResult();
				if (qryAnswer != null) {
					score = numSearch.getScore();
					for (String val : qryAnswer) {
						res = new Result(val, score);
						resultsQueue.add(res);
						this.numResults++;
					}
				} else // No more results available from this query
					break;
			}

			running.set(false);

		} catch (Exception e) { // InterruptedException
			e.printStackTrace();
		} finally {
			this.logStream.println("Thread " + this.name + " collected " + this.numResults + " results.");
		}
	}
	

	/**
	 * Instantiates a categorical similarity search query against an inverted index of sets of tokens.
	 * @param k   The number of results to fetch.
	 * @param results   The queue to collect query results.
	 */
	public void applyCategoricalSearch(int k, ConcurrentLinkedQueue<Result> results) {
		
		// Compute the results
		long duration = System.nanoTime();
		CategoricalSimSearch catSearch = new CategoricalSimSearch(this.idx, logStream);
		
		// Get one extra result to ensure that top-k results can be safely computed
		catSearch.compute(transformedQueryCollection, transformedTargetCollection, k+1, results); 
		
		//Progressive fetching of results
		try {
			List<String> qryAnswer;
			double score;
			Result res;

			running.set(true);
			// Continue fetching results until process gets suspended
			while (running.get()) {  
			// ALTERNATIVE OPTION: until top-k results are retrieved
	//		for (int i = 0; i < collectionSize; i++) { 
	
				if (running.get() == false) {
					break;
				}
	
				TimeUnit.NANOSECONDS.sleep(100);
				qryAnswer = catSearch.getNextResult();
				if (qryAnswer != null) {
					score = catSearch.getScore();
					for (String val : qryAnswer) {
						res = new Result(val, score);
						resultsQueue.add(res);
						this.numResults++;
					}
				} else // No more results available from this query
					break;
			}
	
			running.set(false);
	
		} catch (InterruptedException e) { // InterruptedException
			e.printStackTrace();
		} finally {
			this.logStream.println("Thread " + this.name + " collected " + this.numResults + " results.");
		}
		duration = System.nanoTime() - duration;
		this.logStream.println("Categorical search time: " + duration / 1000000000.0 + " sec.");
		
	}
	
	/**
	 * Executes the specified similarity search query against the index 
	 * and returns results one-by-one.
	 */
	public void run() {
		
		switch (type) {
		case TYPE_CATEGORICAL_TOPK:
			applyCategoricalSearch(this.collectionSize, this.resultsQueue);
			break;
		case TYPE_SPATIAL_KNN:
			//TODO: Specify spatial operation
			break;
		case TYPE_NUMERICAL_TOPK:
			applyNumericalSimSearch(this.index, this.searchingKey, this.resultsQueue);
			break;
		default:
			break;
		}
	}
	
}