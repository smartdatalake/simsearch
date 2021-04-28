package eu.smartdatalake.simsearch.engine.processor.ingested;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import eu.smartdatalake.simsearch.engine.measure.ISimilarity;
import eu.smartdatalake.simsearch.engine.processor.ranking.RankedList;
import eu.smartdatalake.simsearch.manager.ingested.Index;
import eu.smartdatalake.simsearch.manager.ingested.categorical.InvertedIndex;
import eu.smartdatalake.simsearch.manager.ingested.categorical.TokenSetCollection;
import eu.smartdatalake.simsearch.manager.ingested.numerical.BPlusTree;
import eu.smartdatalake.simsearch.manager.ingested.spatial.Location;
import eu.smartdatalake.simsearch.manager.ingested.spatial.RTree;
import eu.smartdatalake.simsearch.Assistant;
import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.Logger;

/**
 * Instantiates a similarity search query of varying type (categorical, string, numerical, spatial, temporal).
 * Applies only against an ingested data source supported by a suitable in-memory index.
 */
public class IndexSimSearch implements Runnable {
	
	Logger log = null;
	Assistant myAssistant;
	int operation;       	//Type of the search query
	String hashKey = null;	// The unique hash key assigned to this search query
	
	Index<Object, Object> index;    //Handle to the underlying index structure (e.g., B+-tree)
	
	long numResults;

	public int collectionSize;
	int topk;
	RankedList resultsQueue;

	// Used in numerical similarity search only
	double searchingKey;
	
	// Used in categorical similarity search only
	TokenSetCollection queryCollection;
	Map<?, ?> origTokenSetCollection;

	// Used in spatial similarity search only
	Location searchLocation;
	
	// Handle to the similarity measure to be used
	ISimilarity<?> simMeasure;
	
	public AtomicBoolean running = new AtomicBoolean(false);
	
	public String name;    //An informative name (operation + dataset + attribute) given to the running thread of similarity search 

	/**
	 * Constructor for CATEGORICAL/TEXTUAL similarity search against against an already constructed index over sets of tokens.
	 * @param operation  The type of the similarity search query (0: CATEGORICAL, 8: TEXTUAL).
	 * @param name  A user-specified name given to the running instance of similarity search.
	 * @param idx  Handle to the inverted index already built over the input data tokens.
	 * @param map  The original collection of tokens (i.e., keywords) specified in the query.
	 * @param queryCollection  The query collection of tokens to search for similarity.
	 * @param topk  The number of the final top-k results.
	 * @param collectionSize  The count of results to fetch.
	 * @param simMeasure  The similarity measure to be used in the search.
	 * @param resultsQueue  Queue to collect query results.
	 * @param hashKey  The unique hash key assigned to this search query.
	 * @param log  Handle to the log file for keeping messages and statistics.
	 */
	public IndexSimSearch(int operation, String name, Index<Object, Object> idx, Map<?, ?> map, TokenSetCollection queryCollection, int topk, int collectionSize, ISimilarity simMeasure, RankedList resultsQueue, String hashKey, Logger log) {

		super();
		this.log = log;
		myAssistant = new Assistant();
		this.operation = operation;
		this.name = name;
		this.simMeasure = simMeasure;		
		this.origTokenSetCollection = map;
		this.queryCollection = queryCollection;
		this.collectionSize = collectionSize;
		this.topk = topk;
		this.index = idx;
		this.resultsQueue = resultsQueue;
		this.numResults = 0;
		this.hashKey = hashKey;
	}
	
	/**
	 * Constructor for NUMERICAL/TEMPORAL similarity search queries against an already constructed index over numerical (or temporal epoch) values.
	 * @param operation  The type of the similarity search query (2: NUMERICAL, 7: TEMPORAL).
	 * @param name  A user-specified name given to the running instance of similarity search.
	 * @param idx   The underlying index.
	 * @param searchingKey    The key (numerical/epoch) value to search against the index.
	 * @param collectionSize  The count of results to fetch.
	 * @param topk  The number of the final top-k results.
	 * @param simMeasure  The similarity measure to be used in the search.
	 * @param resultsQueue   Queue to collect query results.
	 * @param hashKey  The unique hash key assigned to this search query.
	 * @param log  Handle to the log file for keeping messages and statistics.
	 */
	public IndexSimSearch(int operation, String name, Index<Object, Object> idx, double searchingKey, int topk, int collectionSize, ISimilarity simMeasure, RankedList resultsQueue, String hashKey, Logger log) {

		super();
		this.log = log;
		myAssistant = new Assistant();
		this.operation = operation;
		this.name = name;
		this.collectionSize = collectionSize;
		this.topk = topk;
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
	 * @param topk  The number of the final top-k results.
	 * @param collectionSize  The count of results to fetch.
	 * @param simMeasure  The similarity measure to be used in the search.
	 * @param resultsQueue  Queue to collect query results.
	 * @param hashKey  The unique hash key assigned to this search query.
	 * @param log  Handle to the log file for keeping messages and statistics.
	 */
	public IndexSimSearch(int operation, String name, Index<Object, Object> idx, Location queryLocation, int topk, int collectionSize, ISimilarity<?> simMeasure, RankedList resultsQueue, String hashKey, Logger log) {
		
		super();
		this.log = log;
		myAssistant = new Assistant();
		this.operation = operation;
		this.name = name;
		this.collectionSize = collectionSize;
		this.topk = topk;
		this.searchLocation = queryLocation;
		this.index = idx;
		this.resultsQueue = resultsQueue;
		this.numResults = 0;
		this.simMeasure = simMeasure;
		this.hashKey = hashKey;
	}


	/**
	 * Instantiates a numerical similarity search query.
	 * CAUTION! Also used in temporal similarity search since date/time values have been stored as numerical epoch values in the index.
	 * @param idx  The B+-tree index used in the search. It contains (numerical) keys and (string) values for the object identifiers.
	 * @param searchingKey  The search key against the index (i.e., the numerical value specified by the query).
	 * @return  A boolean value: True, if the query is still running; otherwise, False.
	 */
	public boolean applyNumericalSimSearch(Index<Object, Object> idx, Double searchingKey) {
		
		NumericalSimSearch<Double, String> numSearch =  new NumericalSimSearch<Double, String>((BPlusTree<Double, String>)idx, searchingKey, (ISimilarity<Double>) this.simMeasure, this.resultsQueue, this.log);
		
		// Compute results for this numerical similarity search query
		// This kind of search can also provide results progressively
		// CAUTION! The number of items to fetch is actually is >> k specified in the top-k query.
		this.numResults = numSearch.compute(topk, collectionSize);
		
		running.set(false);
		
		return running.get();
	}
	
	
	/**
	 * Instantiates a spatial similarity search query.
	 * @param idx  The R-tree index used in the search. It contains (MBR) keys and (id, location) values for the objects.
	 * @param searchLoc  The query location to search for k-NN similarities against the index.
	 * @return  A boolean value: True, if the query is still running; otherwise, False.
	 */
	public boolean applySpatialSimSearch(Index<Object, Object> idx, Location searchLoc) {
		
		SpatialSimSearch<String, Location> spatialSearch =  new SpatialSimSearch<String, Location>((RTree<String, Location>)idx, searchLoc, (ISimilarity<Location>) this.simMeasure, this.resultsQueue, this.log);
		
		// Compute results for this spatial similarity search query
		// CAUTION! The number of nearest neighbors to fetch is actually is M >> k specified in the top-k query.
		spatialSearch.compute(topk, collectionSize);
		
		//Progressive fetching of results
		try {
			int n = 0;
			running.set(true);

			// Continue fetching results until process gets suspended
			while (running.get()) {  

				if (running.get() == false)
					break;

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
	 * @return  A boolean value: True, if the query is still running; otherwise, False.
	 */
	public boolean applyCategoricalSimSearch(Index<Object, Object> idx, TokenSetCollection queryCollection) {
		
		// Handle to the inverted index built on target dataset
		InvertedIndex index = (InvertedIndex) idx;
				
		CategoricalSimSearch catSearch = new CategoricalSimSearch(index.idx, (ISimilarity<?>) this.simMeasure, this.log);
		
		// Compute results for this categorical similarity search query
		// This kind of search can also provide results progressively (depending on the similarity upper bound of any future matches)
		// CAUTION! The number of items to fetch is actually is >> k specified in the top-k query.
		this.numResults = catSearch.compute(index.getTransformedCollection(queryCollection), index.transformedTargetCollection, origTokenSetCollection, topk, collectionSize, this.resultsQueue);
		
		running.set(false);

		return running.get();
	}
	
	public boolean applyTextualSimSearch(Index<Object, Object> idx, TokenSetCollection queryCollection) {
		
		// Handle to the inverted index built on target dataset
		InvertedIndex index = (InvertedIndex) idx;
				
		CategoricalSimSearch catSearch = new CategoricalSimSearch(index.idx, (ISimilarity<?>) this.simMeasure, this.log);
		
		// Compute results for this textual (string) similarity search query
		// CAUTION! The number of items to fetch is actually is >> k specified in the top-k query.
		this.numResults = catSearch.compute(index.getTransformedCollection(queryCollection), index.transformedTargetCollection, origTokenSetCollection, topk, collectionSize, this.resultsQueue);
		
		running.set(false);

		return running.get();
	}
	
	/**
	 * Executes the specified similarity search query against the index and returns results one-by-one.
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
		case Constants.TEMPORAL_TOPK:  // CAUTION! Actually instantiates numerical similarity search over epoch values in the index
			running = applyNumericalSimSearch(this.index, this.searchingKey);
			break;
		case Constants.TEXTUAL_TOPK:	// CAUTION! Actually instantiates categorical similarity search over qgram values in the index
			running = applyCategoricalSimSearch(this.index, this.queryCollection);
			break;
		default:
			break;
		}
		
		duration = System.nanoTime() - duration;
		if (!running)
			this.log.writeln("Query [" + myAssistant.decodeOperation(this.operation) + "] " + this.name.substring(this.name.indexOf(".") + 1) + " (ingested) returned " + this.numResults + " results in " + duration / 1000000000.0 + " sec.");
	}
	
}
