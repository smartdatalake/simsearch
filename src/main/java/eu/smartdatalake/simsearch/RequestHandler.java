package eu.smartdatalake.simsearch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.locationtech.jts.geom.Geometry;

import eu.smartdatalake.simsearch.csv.Index;
import eu.smartdatalake.simsearch.csv.SimSearch;
import eu.smartdatalake.simsearch.csv.categorical.TokenSetCollection;
import eu.smartdatalake.simsearch.csv.categorical.TokenSetCollectionReader;
import eu.smartdatalake.simsearch.csv.numerical.BPlusTree;
import eu.smartdatalake.simsearch.csv.numerical.INormal;
import eu.smartdatalake.simsearch.csv.spatial.Location;
import eu.smartdatalake.simsearch.csv.spatial.LocationReader;
import eu.smartdatalake.simsearch.csv.spatial.RTree;
import eu.smartdatalake.simsearch.jdbc.CategoricalValueFinder;
import eu.smartdatalake.simsearch.jdbc.JdbcConnector;
import eu.smartdatalake.simsearch.jdbc.NumericalValueFinder;
import eu.smartdatalake.simsearch.jdbc.SimSearchQuery;
import eu.smartdatalake.simsearch.jdbc.SpatialValueFinder;
import eu.smartdatalake.simsearch.measure.CategoricalDistance;
import eu.smartdatalake.simsearch.measure.DecayedSimilarity;
import eu.smartdatalake.simsearch.measure.ISimilarity;
import eu.smartdatalake.simsearch.measure.NumericalDistance;
import eu.smartdatalake.simsearch.measure.SpatialDistance;
import eu.smartdatalake.simsearch.ranking.IRankAggregator;
import eu.smartdatalake.simsearch.ranking.NoRandomAccessRanking;
import eu.smartdatalake.simsearch.ranking.PartialRandomAccessRanking;
import eu.smartdatalake.simsearch.ranking.RankedResult;
import eu.smartdatalake.simsearch.ranking.ResultMatrix;
import eu.smartdatalake.simsearch.ranking.ResultPair;
import eu.smartdatalake.simsearch.ranking.ThresholdRanking;

/**
 * Handles a new multi-attribute similarity search request. a new instance is invoked by the coordinator.
 */
public class RequestHandler {

	Logger log = null;
	Assistant myAssistant;
	
	Map<String, Thread> tasks;
	Map<String, ISimilarity> similarities;
	Map<String, Double[]> weights;
	Map<String, INormal> normalizations;
	Map<String, HashMap<?,?>> datasets;
	Map<String, HashMap<?,?>> lookups;
	Map<String, DataSource> dataSources;
	Map<String, Index> indices;
	Map<String, DatasetIdentifier> datasetIdentifiers;
	Map<String, IValueFinder> dbValueFinders;
	
	// List of queues that collect results from each running task
	Map<String, ConcurrentLinkedQueue<PartialResult>> queues;
	
	// List of atomic booleans to control execution of the various threads
	Map<String, AtomicBoolean> runControl;

	/**
	 * Constructor
	 * @param dataSources
	 * @param datasetIdentifiers
	 * @param datasets
	 * @param indices
	 * @param normalizations
	 * @param log  Handle to the log file for notifications and execution statistics.
	 */
	public RequestHandler(Map<String, DataSource> dataSources, Map<String, DatasetIdentifier> datasetIdentifiers, Map<String, HashMap<?,?>> datasets, Map<String, Index> indices, Map<String, INormal> normalizations, Logger log) {
			
		this.normalizations = normalizations;
		this.datasets = datasets;
		this.dataSources = dataSources;
		this.datasetIdentifiers = datasetIdentifiers;
		this.indices = indices;
		this.log = log;	
		myAssistant = new Assistant();
		
	    // New instances of various constructs specifically for this request
		queues = new HashMap<String, ConcurrentLinkedQueue<PartialResult>>();
		tasks = new HashMap<String, Thread>();
		similarities = new HashMap<String, ISimilarity>();
		weights = new HashMap<String, Double[]>();
		lookups = new HashMap<String, HashMap<?,?>>();
		runControl = new HashMap<String, AtomicBoolean>();
		dbValueFinders = new HashMap<String, IValueFinder>();
	}

	
	/**
	 * Finds the internal identifier used for the data of a given attribute.
	 * @param column   The name of the attribute.
	 * @return  The dataset identifier.
	 */
	private DatasetIdentifier findIdentifier(String column) {
		
		for (Entry<String, DatasetIdentifier> entry : datasetIdentifiers.entrySet()) {
            if (entry.getValue().getColumnName().equals(column)) {
            	return entry.getValue();
            }
        }
	
		return null;
	}
	

	/**
	 * Provides the maximum value in a numerical attribute
	 * @param dbConnector  The JDBC connection specification to a DBMS (if applicable).
	 * @param id  The identifier of the attribute in a dataset.
	 * @return  The maximum numerical value available in the specified attribute.
	 */
	private Double getMaxNumValue(JdbcConnector dbConnector, DatasetIdentifier id) {
		
		if (dbConnector != null)  {	// Querying against a DBMS
			return (Double)dbConnector.findSingletonValue("SELECT max(" + id.colName + ") FROM " + id.getDatasetName() + ";");
		}
		else {				// Querying against in-memory indices over a CSV file
			BPlusTree<Double, String> index = (BPlusTree<Double, String>) indices.get(id.getHashKey());
			return index.calcMaxKey();
		}
	}

	
	/**
	 * Calculate the exact similarity score for a given object with random access to the underlying datasets.
	 * @param id  The object identifier.
	 * @param w  The identifier of the weight combination to be applied on the scores.
	 * @return  The exact aggregated score based on all attribute values.
	 */
	private double getExactScoreRandomAccess(String id, int w) {
		
		double score = 0.0;
		for (String task : tasks.keySet()) {	
			// Need to perform random access for all attributes
			Object val = this.datasets.get(task).get(id);   // Access attribute value
			//CAUTION! Weighted scores used in aggregation
			if (val != null) {   // If value is not null, then update score accordingly
				if (this.normalizations.get(task) != null)   // Apply normalization, if specified
					score += this.weights.get(task)[w] * this.similarities.get(task).calc(this.normalizations.get(task).normalize(val));
				else
					score += this.weights.get(task)[w] * this.similarities.get(task).calc(val);
			}
		}
		return score;
	}
	
	
	/**
	 * Given a user-specified JSON configuration, execute the various similarity search queries and provide the ranked aggregated results.
	 * @param config   JSON configuration that provides the multi-facet query specifications.
	 * @return   A JSON-formatted response with the ranked results.
	 */
	public SearchResponse[] search(JSONObject config) {
		
		long duration;
		SearchResponse[] responses;
		
		IRankAggregator aggregator = null;
		String rankingMethod = Constants.RANKING_METHOD;   	// Threshold is the default ranking algorithm

		// Specifications for output CSV file (if applicable)
		OutputWriter outCSVWriter = new OutputWriter(config);
	
		int topk = 0;   // top-k value for ranked aggregated results
		    
		JSONArray queries = null;
		Double[] arrWeights = null;
		
		try {
			log.writeln("********************** New search request ... **********************");
			topk = Integer.parseInt(String.valueOf(config.get("k")));
			// Allow only positive integers for top-k parameter
			if (topk < 1)
				throw new NumberFormatException();
			
			// Determine the ranking algorithm to be used for top-k aggregation
			if (config.get("algorithm") != null)
				rankingMethod = String.valueOf(config.get("algorithm"));
			
			// Append to already existing log file (created previously during the indexing phase
			if (config.get("log_file") != null) {
				String logFile = String.valueOf(config.get("log_file"));	
				if (log == null) {   // Use already existing log file
					logFile = String.valueOf(config.get("log_file"));
					log = new Logger(logFile, true);
				}
			}
			
			// Array of specified queries
			queries = (JSONArray) config.get("queries");
			
		} catch(NumberFormatException e) {
			e.printStackTrace();
			responses = new SearchResponse[1];
			SearchResponse response = new SearchResponse();
			log.writeln("Search request discarded due to illegal specification of parameters.");
			response.setNotification("Please specify a positive integer value for k. Search request dismissed.");
			responses[0] = response;
			return responses;
		}
		
		// JDBC connections opened during this request
		List<JdbcConnector> openJdbcConnections = new ArrayList<JdbcConnector>();
		
	    // Iterate over the specified queries
		if (queries != null) {
			
	        Iterator it = queries.iterator();
	        while (it.hasNext()) {

	        	JSONObject queryConfig = (JSONObject) it.next();
				String colValueName = String.valueOf(queryConfig.get("search_column"));
				String colKeyName = null;

				//DatasetIdentifier to be used for all constructs built for this attribute
				DatasetIdentifier id = findIdentifier(colValueName);				
				
				if (id == null) {
					log.writeln("No dataset with attribute " + colValueName + " is available for search.");
					throw new NullPointerException("No dataset with attribute " + colValueName + " is available for search.");
				}
					
				// Get the respective connection details to this dataset
				DataSource dataSource = id.getDataSource();
				JdbcConnector jdbcConn = null;
				if (dataSource.getJdbcConnPool() != null) {  // Initialize a new JDBC connection from the pool
					jdbcConn = dataSource.getJdbcConnPool().initDbConnector();
					openJdbcConnections.add(jdbcConn);
				}
				
				// operation
				String operation = myAssistant.descOperation(id.operation);
				
				// Assign a name to this operation
				String name = operation + " " + id.getDatasetName();
				
				// Exponential decay factor lambda to be used in similarity calculations
				double decay = Constants.DECAY_FACTOR;
				if (queryConfig.get("decay") != null)
					decay = Double.parseDouble(String.valueOf(queryConfig.get("decay")));
				
				// Similarity measure: By default, decayed similarity will be used in all types of search
				ISimilarity<?> simMeasure = null;
			
				// Queue that will be used to collect query results
				ConcurrentLinkedQueue<PartialResult> resultsQueue = new ConcurrentLinkedQueue<PartialResult>();
				
				// WEIGHT: Array of specified weights to apply on a specific facet
				arrWeights = myAssistant.arrayJSON2Double((JSONArray) queryConfig.get("weights"));
				weights.put(id.getHashKey(), arrWeights);
				
				// DatasetIdentifier to be used for all constructs built for this attribute
				// This is normally added at indexing stage; should be added here only when querying against a DBMS
				// On-the-fly look-up also occurs when the Partial Random Access method is applied
				if (!datasets.containsKey(id.getHashKey()) || (rankingMethod.equals("partial_random_access")))  {
					// Use a generated hash key of the column as a reference of values to be looked up for this attribute
					lookups.put(id.getHashKey(), new HashMap<String, Object>());
					log.writeln("Look-up table for " + id.getColumnName() + " will be created on-the-fly during ranking.");
				}
				else {  // Otherwise, the lookup is the original collection of attribute values
					lookups.put(id.getHashKey(), datasets.get(id.getHashKey()));
				}
				
				// settings for top-k similarity search on categorical values
				if (operation.equalsIgnoreCase("categorical_topk")) {
					
					Thread threadCatSearch = null;
					
					// QUERY SPECIFICATION
					JSONArray jsonKeywords = (JSONArray) queryConfig.get("search_value");
					
					// Keywords should be specified as a JSON array
					if (jsonKeywords.isEmpty()) {
						log.writeln("Operation " + operation + " using a NULL or invalid search value is not supported! This query facet will be ignored.");
						continue;
					}
					String[] searchKeywords = new String[jsonKeywords.size()];
				    for(int j=0; j<jsonKeywords.size(); j++) {
				    	searchKeywords[j] = String.valueOf(jsonKeywords.get(j));
				    }
				    				
					// Inflate top-k value in order to fetch enough candidates from categorical search
					int collectionSize = Constants.INFLATION_FACTOR * topk; // FIXME: Avoid hard-coded value 

					TokenSetCollectionReader reader = new TokenSetCollectionReader();
					TokenSetCollection queryCollection = null;
					
					// This will create a collection for the query only
					queryCollection = reader.createFromQueryKeywords(searchKeywords, log);

					// Jaccard distance is applied on categorical (textual) values
					// Similarity also indicates the corresponding task serial number
					simMeasure = new DecayedSimilarity(new CategoricalDistance(queryCollection.sets.get("querySet")), decay, tasks.size());
					similarities.put(id.getHashKey(), simMeasure);
					
					// Create an instance of the categorical search query (CATEGORICAL_TOPK = 0)
					if (jdbcConn != null)  {	// Querying against a DBMS
						id.setOperation(Constants.CATEGORICAL_TOPK);
						// FIXME: Separator for search keywords must be ";" in this case
						SimSearchQuery catSearch = new SimSearchQuery(jdbcConn, Constants.CATEGORICAL_TOPK, id.getDatasetName(), colKeyName, colValueName, String.join(";", searchKeywords), collectionSize, simMeasure, resultsQueue, lookups, id.getHashKey(), log);
						dbValueFinders.put(id.getHashKey(), new CategoricalValueFinder(jdbcConn, catSearch.sqlValueRetrievalTemplate));
						threadCatSearch = new Thread(catSearch);
						runControl.put(id.getHashKey(), catSearch.running);
					}
					else {			// Querying against in-memory indices over a CSV file
						SimSearch catSearch = new SimSearch(Constants.CATEGORICAL_TOPK, name, indices.get(id.getHashKey()), datasets.get(id.getHashKey()), queryCollection, collectionSize, simMeasure, resultsQueue, id.getHashKey(), log);
						threadCatSearch = new Thread(catSearch);
						runControl.put(id.getHashKey(), catSearch.running);
					}
					
					threadCatSearch.setName(name);
					tasks.put(id.getHashKey(), threadCatSearch);
					queues.put(id.getHashKey(), resultsQueue);
				}
				// settings for top-k similarity search on numerical values
				else if (operation.equalsIgnoreCase("numerical_topk")) {
					
					Thread threadNumSearch = null;
					
					// QUERY SPECIFICATION
					String val = String.valueOf(queryConfig.get("search_value"));
					// In case the "max" value is specified in search, identify this numerical value in the dataset
					// Otherwise, get the user-specified numerical value to search
					Double searchingKey = (val.equalsIgnoreCase("max") ? getMaxNumValue(jdbcConn, id) : Double.parseDouble(val));
					// Check for NULL or invalid numerical value
					if ((searchingKey == null) || (searchingKey.isNaN())) {
						log.writeln("Operation " + operation + " using a NULL or invalid search value is not supported! This query facet will be ignored.");
						continue;
					}
					
					// Check whether data has been normalized during indexing
					INormal normal = normalizations.get(id.getHashKey());

					// The search key must be also normalized as was the input dataset
					if (normal != null) {
						searchingKey = normal.normalize(searchingKey);
						log.writeln("Normalized search key:" + searchingKey);
					}

					// Absolute difference is used for estimating distance (and thus, similarity) of numerical values
					// Similarity also indicates the corresponding task serial number
					simMeasure = new DecayedSimilarity(new NumericalDistance(searchingKey), decay, tasks.size());
					similarities.put(id.getHashKey(), simMeasure);
					
					// Inflate top-k value in order to fetch enough candidates from categorical search
					int collectionSize = Constants.INFLATION_FACTOR * topk; // FIXME: Avoid hard-coded value 
					
					// Create an instance of the numerical search query (NUMERICAL_TOPK = 2)
					if (jdbcConn != null)  {	// Querying against a DBMS
						id.setOperation(Constants.NUMERICAL_TOPK);
						SimSearchQuery numSearch = new SimSearchQuery(jdbcConn, Constants.NUMERICAL_TOPK, id.getDatasetName(), colKeyName, colValueName, String.valueOf(searchingKey), collectionSize, simMeasure, resultsQueue, lookups, id.getHashKey(), log);
						dbValueFinders.put(id.getHashKey(), new NumericalValueFinder(jdbcConn, numSearch.sqlValueRetrievalTemplate));
						threadNumSearch = new Thread(numSearch);
						runControl.put(id.getHashKey(), numSearch.running);	
					}
					else {				// Querying against in-memory indices over a CSV file						
						// Identify the B+-tree already built for this attribute
						BPlusTree<Double, String> index = (BPlusTree<Double, String>) indices.get(id.getHashKey());
						// collectionSize = -1 -> no prefixed bound on the number of results to fetch from numerical similarity search
						SimSearch numSearch = new SimSearch(Constants.NUMERICAL_TOPK, name, index, searchingKey, collectionSize, simMeasure, resultsQueue, id.getHashKey(), log);
						threadNumSearch = new Thread(numSearch);
						runControl.put(id.getHashKey(), numSearch.running);		
					}
					
					threadNumSearch.setName(name);
					tasks.put(id.getHashKey(), threadNumSearch);
					queues.put(id.getHashKey(), resultsQueue);
				}
				// settings for k-NN similarity search on spatial locations
				else if (operation.equalsIgnoreCase("spatial_knn")) {
					
					Thread threadGeoSearch = null;
					
					// QUERY SPECIFICATION
					String queryWKT = String.valueOf(queryConfig.get("search_value"));
					LocationReader locReader = new LocationReader();
					Geometry queryPoint = locReader.WKT2Geometry(queryWKT);
					// Check for NULL query value
					if (queryPoint == null) {
						log.writeln("Operation " + operation + " using a NULL or invalid search value is not supported! This query facet will be ignored.");
						continue;
					}
					Location queryLocation = new Location("QUERY", queryPoint);

					// By default, Haversine distance is used in similarity calculations between spatial locations
					// Similarity also indicates the corresponding task serial number
					simMeasure = new DecayedSimilarity(new SpatialDistance(queryPoint), decay, tasks.size());
					similarities.put(id.getHashKey(), simMeasure);
					
					// Inflate top-k value in order to fetch enough candidates from categorical search
					int collectionSize = Constants.INFLATION_FACTOR * topk; // FIXME: Avoid hard-coded value 
					
					// Create an instance of the spatial similarity search query (SPATIAL_KNN = 1)
					if (jdbcConn != null)  {	// Querying against a DBMS
						id.setOperation(Constants.SPATIAL_KNN);
						SimSearchQuery geoSearch = new SimSearchQuery(jdbcConn, Constants.SPATIAL_KNN, id.getDatasetName(), colKeyName, colValueName, queryPoint.toText(), collectionSize, simMeasure, resultsQueue, lookups, id.getHashKey(), log);
						dbValueFinders.put(id.getHashKey(), new SpatialValueFinder(jdbcConn, geoSearch.sqlValueRetrievalTemplate));
						threadGeoSearch = new Thread(geoSearch);
						runControl.put(id.getHashKey(), geoSearch.running);
					}
					else {			// Querying against in-memory indices over a CSV file	
						// Identify the R-tree already built for this attribute
						RTree<String, Location> index = (RTree<String, Location>) indices.get(id.getHashKey());
						SimSearch geoSearch = new SimSearch(Constants.SPATIAL_KNN, name, index, queryLocation, collectionSize, simMeasure, resultsQueue, id.getHashKey(), log);
						threadGeoSearch = new Thread(geoSearch);
						runControl.put(id.getHashKey(), geoSearch.running);
					}
					
					threadGeoSearch.setName(name);
					tasks.put(id.getHashKey(), threadGeoSearch);
					queues.put(id.getHashKey(), resultsQueue);		
				}
				//TODO: Include other operations...
				else {
					log.writeln("Unknown operation specified: " + operation);
				}			
			}	
		}

		// Start all tasks; each query will now start fetching results
		for (Entry<String, Thread> task: tasks.entrySet()) {
			task.getValue().start();
		}
	
		// Perform the ranked aggregation process
		duration = System.nanoTime();

		// Instantiate the rank aggregator that will handle results from the various threads
		// Execute rank aggregation separately for all combination of weights
		switch(rankingMethod){
		case "no_random_access":
			aggregator = new NoRandomAccessRanking(datasetIdentifiers, lookups, similarities, weights, normalizations, tasks, queues, runControl, topk, log);
			break;
		case "partial_random_access":
			aggregator = new PartialRandomAccessRanking(datasetIdentifiers, lookups, similarities, weights, normalizations, tasks, queues, runControl, topk, log);
			break;
		case "threshold":
			aggregator = new ThresholdRanking(datasetIdentifiers, lookups, similarities, weights, normalizations, tasks, queues, dbValueFinders, runControl, topk, log);
			break;
		default:
			log.writeln("No ranking method specified!");
			break;
		}
		responses = null;
			
		// Collect results that may be issued as JSON
		RankedResult[][] results = aggregator.proc();
		
		// Number of combinations of weights that have been applied
		int numWeights = weights.entrySet().iterator().next().getValue().length;
		responses = new SearchResponse[numWeights];
			
		// Produce final results for each combination of weights
		for (int w = 0; w < numWeights; w++) {
			Map<String, Double> curWeights = new HashMap<String, Double>();
			Iterator it = weights.entrySet().iterator();
		    while (it.hasNext()) {
		    	Map.Entry pair = (Map.Entry)it.next();
		    	curWeights.put((String) pair.getKey(), ((Double[])pair.getValue())[w]);
		    }
		    
/************************USER FOR EXPERIMENTS ONLY********************************		
		    // FIXME: Change the estimated scores with the exact ones
		    if (!rankingMethod.equals("threshold")) {
//		    	System.out.println();
		    	for (int p = 0; p < results[w].length; p++) {
//		    		System.out.print(results[w][p].getScore());
		    		results[w][p].setScore(getExactScoreRandomAccess(results[w][p].getId(), w));	
//		    		System.out.println(" --> " + results[w][p].getScore());
		    	}
		    }
*********************************************************************************/			    
	    
			// Prepare a response to this combination of weights
			SearchResponse response = new SearchResponse();
			response.setWeights(curWeights.values().toArray(new Double[0]));

			// Post-processing of the search results in order to calculate their pairwise similarity
			ResultMatrix matrixCalculator = new ResultMatrix(this.datasetIdentifiers, this.lookups, this.similarities, curWeights, this.normalizations);
			ResultPair[] simMatrix = matrixCalculator.calc(results[w]);
			
			// Populate overall response to this multi-facet search query
			response.setRankedResults(results[w]);
			response.setSimilarityMatrix(simMatrix);
			
			if (results[w].length < topk)
				response.setNotification("Search inconclusive because at least one query facet failed to provide a sufficient number of candidates.");
			else
				response.setNotification("");
			
			responses[w] = response;	
			
			// Print results to a CSV file (if applicable)
			if (outCSVWriter.isSet()) {
				outCSVWriter.writeResults(results[w]);
			}	    
		}
		
		// Close output writer to CSV (if applicable)
		if (outCSVWriter.isSet())
			outCSVWriter.close();
		
		duration = System.nanoTime() - duration;
		System.out.print((duration/ 1000000000.0) + ",");   // Execution cost for experimental results
		log.writeln("Rank aggregation [" + rankingMethod + "] issued " + responses[0].getRankedResults().length + " results. Processing time: " + duration / 1000000000.0 + " sec.");
		
		// Close any JDBC connections
		for (JdbcConnector jdbcConn: openJdbcConnections)
			jdbcConn.closeConnection();
		
		// Response can be formatted as JSON
		return responses;
	}

}
