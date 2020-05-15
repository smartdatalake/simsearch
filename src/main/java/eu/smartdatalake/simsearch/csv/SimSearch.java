package eu.smartdatalake.simsearch.csv;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import eu.smartdatalake.simsearch.csv.numerical.BPlusTree;
import eu.smartdatalake.simsearch.csv.numerical.NumericalSimSearch;
import eu.smartdatalake.simsearch.csv.spatial.Location;
import eu.smartdatalake.simsearch.csv.spatial.RTree;
import eu.smartdatalake.simsearch.csv.spatial.kNNSearch;
import eu.smartdatalake.simsearch.measure.ISimilarity;
import eu.smartdatalake.simsearch.Assistant;
import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.PartialResult;
import eu.smartdatalake.simsearch.csv.categorical.CategoricalSimSearch;
import eu.smartdatalake.simsearch.csv.categorical.InvertedIndex;
import eu.smartdatalake.simsearch.csv.categorical.TokenSetCollection;

/**
 * Instantiates a similarity search query of varying type (categorical, numerical, spatial) against an in-memory data source.
 */
public class SimSearch implements Runnable {
	
	Logger log = null;
	Assistant myAssistant;
	int operation;       	//Type of the search query
	String hashKey = null;	// The unique hash key assigned to this search query
	
	Index<Object, Object> index;    //Handle to the underlying index structure (e.g., B+-tree)
	
	long numResults;

	public int collectionSize;
	ConcurrentLinkedQueue<PartialResult> resultsQueue;

	// Used in numerical similarity search only
	double searchingKey;
	
	// Used in categorical similarity search only
	TokenSetCollection queryCollection;
	HashMap<?, ?> origTokenSetCollection;

	// Used in spatial similarity search only
	Location searchLocation;
	
	// Handle to the similarity measure to be used
	ISimilarity<?> simMeasure;
	
	public AtomicBoolean running = new AtomicBoolean(false);
	
	public String name;    //An informative name given to the running instance of similarity search 

	/**
	 * Constructor for CATEGORICAL similarity search against against an already constructed index over sets of tokens.
	 * @param operation  The type of the similarity search query (0: CATEGORICAL).
	 * @param name  A user-specified name given to the running instance of similarity search.
	 * @param idx  Handle to the inverted index already built over the input data tokens.
	 * @param queryCollection  The query collection of tokens to search for similarity.
	 * @param collectionSize  The count of results to fetch.
	 * @param simMeasure  The similarity measure to be used in the search.
	 * @param resultsQueue  Queue to collect query results
	 * @param log  Handle to the log file for keeping messages and statistics
	 */
	public SimSearch(int operation, String name, Index<Object, Object> idx, HashMap<?, ?> origTokenSetCollection, TokenSetCollection queryCollection, int collectionSize, ISimilarity simMeasure, ConcurrentLinkedQueue<PartialResult> resultsQueue, String hashKey, Logger log) {

		super();
		this.log = log;
		myAssistant = new Assistant();
		this.operation = operation;
		this.name = name;
		this.simMeasure = simMeasure;		
		this.origTokenSetCollection = origTokenSetCollection;
		this.queryCollection = queryCollection;
		this.collectionSize = collectionSize;
		this.index = idx;
		this.resultsQueue = resultsQueue;
		this.numResults = 0;
		this.hashKey = hashKey;
	}
	
	/**
	 * Constructor for NUMERICAL similarity search queries against an already constructed index over numerical values.
	 * @param operation  The type of the similarity search query (2: NUMERICAL).
	 * @param name  A user-specified name given to the running instance of similarity search.
	 * @param idx   The underlying index.
	 * @param searchingKey    The key value to search against the index.
	 * @param collectionSize  The count of results to fetch.
	 * @param simMeasure  The similarity measure to be used in the search.
	 * @param resultsQueue   Queue to collect query results
	 * @param log  Handle to the log file for keeping messages and statistics
	 */
	public SimSearch(int operation, String name, Index<Object, Object> idx, double searchingKey, int collectionSize, ISimilarity simMeasure, ConcurrentLinkedQueue<PartialResult> resultsQueue, String hashKey, Logger log) {

		super();
		this.log = log;
		myAssistant = new Assistant();
		this.operation = operation;
		this.name = name;
		this.collectionSize = collectionSize;
		this.searchingKey = searchingKey;
		this.index = idx;
		this.resultsQueue = resultsQueue;
		this.numResults = 0;
		this.simMeasure = simMeasure;
		this.hashKey = hashKey;
	}

	/**
	 * Constructor for SPATIAL similarity search queries against an already constructed index over POINT locations.
	 * @param operation  The type of the similarity search query (1: SPATIAL).
	 * @param name  A user-specified name given to the running instance of similarity search.
	 * @param idx   The underlying index.
	 * @param queryLocation  The query location to be used for searching against the index.
	 * @param collectionSize  The count of results to fetch.
	 * @param simMeasure  The similarity measure to be used in the search.
	 * @param resultsQueue  Queue to collect query results
	 * @param log  Handle to the log file for keeping messages and statistics
	 */
	public SimSearch(int operation, String name, Index<Object, Object> idx, Location queryLocation, int collectionSize, ISimilarity<?> simMeasure, ConcurrentLinkedQueue<PartialResult> resultsQueue, String hashKey, Logger log) {
		
		super();
		this.log = log;
		myAssistant = new Assistant();
		this.operation = operation;
		this.name = name;
		this.collectionSize = collectionSize;
		this.searchLocation = queryLocation;
		this.index = idx;
		this.resultsQueue = resultsQueue;
		this.numResults = 0;
		this.simMeasure = simMeasure;
		this.hashKey = hashKey;
	}


	/**
	 * Instantiates a numerical similarity search query.
	 * @param idx  The B+-tree index used in the search. It contains (numerical) keys and (string) values for the object datasetIdentifiers.
	 * @param searchingKey  The search key against the index (i.e., the numerical value specified by the query).
	 * @param partialResults  The queue to collect query results.
	 */
	public boolean applyNumericalSimSearch(Index<Object, Object> idx, Double searchingKey) {
		
		NumericalSimSearch<Double, String> numSearch =  new NumericalSimSearch<Double, String>((BPlusTree<Double, String>)idx, searchingKey, (ISimilarity<Double>) this.simMeasure, this.resultsQueue, this.log);
		
		// Compute results for this numerical similarity search query
		// This kind of search can also provide results progressively
		// CAUTION! The number of items to fetch is actually is >> k specified in the top-k query.
		this.numResults = numSearch.compute(collectionSize);

		running.set(false);
		
		return running.get();
	}
	

	/**
	 * Instantiates a spatial similarity search query.
	 * @param idx  The R-tree index used in the search. It contains (MBR) keys and (id, location) values for the objects.
	 * @param searchLoc  The query location to search for k-NN similarities against the index.
	 * @param partialResults  The queue to collect query results.
	 */
	public boolean applySpatialSimSearch(Index<Object, Object> idx, Location searchLoc) {
		
		kNNSearch<String, Location> spatialSearch =  new kNNSearch<String, Location>((RTree<String, Location>)idx, searchLoc, (ISimilarity<Location>) this.simMeasure, this.resultsQueue, this.log);
		
		// Compute results for this spatial similarity search query
		// CAUTION! The number of nearest neighbors to fetch is actually is >> k specified in the top-k query.
		spatialSearch.compute(collectionSize);
		
		//Progressive fetching of results
		try {
			int n = 0;

			running.set(true);

			// Continue fetching results until process gets suspended
			while (running.get()) {  

				if (running.get() == false) {
					break;
				}

				TimeUnit.NANOSECONDS.sleep(100);
				if ((n = spatialSearch.fetchNextBatch()) > 0 )
					this.numResults += n;
				else  // No more results available from this query
					break;
			}

			running.set(false);

		} catch (Exception e) { // InterruptedException
			e.printStackTrace();
		}
		
		return running.get();
	}

	
	/**
	 * Instantiates a categorical similarity search query against an inverted index of sets of tokens.
	 * @param idx  The inverted index previously built over the target data (i.e., sets of tokens identified on a user-specified attribute).
	 * @param queryCollection  The collection of query tokens to search in the dataset.
	 * @param collectionSize   The number of results to fetch.
	 * @param partialResults   The queue to collect query results.
	 */
	public boolean applyCategoricalSimSearch(Index<Object, Object> idx, TokenSetCollection queryCollection) {
		
		// Handle to the inverted index built on target dataset
		InvertedIndex index = (InvertedIndex) idx;
				
		CategoricalSimSearch catSearch = new CategoricalSimSearch(index.idx, (ISimilarity<?>) this.simMeasure, this.log);
		
		// Compute results for this categorical similarity search query
		// This kind of search can also provide results progressively (depending on the similarity upper bound of any future matches)
		// CAUTION! The number of items to fetch is actually is >> k specified in the top-k query.
		this.numResults = catSearch.compute(index.getTransformedCollection(queryCollection), index.transformedTargetCollection, origTokenSetCollection, collectionSize, this.resultsQueue);
		
		running.set(false);

		return running.get();
	}
	
	
	/**
	 * Executes the specified similarity search query against the index 
	 * and returns results one-by-one.
	 */
	public void run() {
		
		long duration = System.nanoTime();
		boolean running = true;
		switch (this.operation) {
		case Constants.CATEGORICAL_TOPK:
			running = applyCategoricalSimSearch(this.index, this.queryCollection);
			break;
		case Constants.SPATIAL_KNN:
			running = applySpatialSimSearch(this.index, this.searchLocation);
			break;
		case Constants.NUMERICAL_TOPK:
			running = applyNumericalSimSearch(this.index, this.searchingKey);
			break;
		default:
			break;
		}
		duration = System.nanoTime() - duration;
		if (!running)
			this.log.writeln("Query [" + myAssistant.descOperation(this.operation) + "] " + this.hashKey + " (CSV) returned " + this.numResults + " results in " + duration / 1000000000.0 + " sec.");
	}
	
}