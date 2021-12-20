package eu.smartdatalake.simsearch.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.postgis.PGgeometry;
import org.postgresql.util.PGobject;

import eu.smartdatalake.simsearch.Assistant;
import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.engine.measure.CategoricalDistance;
import eu.smartdatalake.simsearch.engine.measure.DecayedSimilarity;
import eu.smartdatalake.simsearch.engine.measure.ISimilarity;
import eu.smartdatalake.simsearch.engine.measure.NumericalDistance;
import eu.smartdatalake.simsearch.engine.measure.SpatialDistance;
import eu.smartdatalake.simsearch.engine.processor.IValueFinder;
import eu.smartdatalake.simsearch.engine.processor.ingested.IndexSimSearch;
import eu.smartdatalake.simsearch.engine.processor.insitu.ElasticSearchRestQuery;
import eu.smartdatalake.simsearch.engine.processor.insitu.SimSearchJdbcQuery;
import eu.smartdatalake.simsearch.engine.processor.insitu.SimSearchRestQuery;
import eu.smartdatalake.simsearch.engine.processor.ranking.IRankAggregator;
import eu.smartdatalake.simsearch.engine.processor.ranking.NoRandomAccessRanking;
import eu.smartdatalake.simsearch.engine.processor.ranking.PartialRandomAccessRanking;
import eu.smartdatalake.simsearch.engine.processor.ranking.RankedList;
import eu.smartdatalake.simsearch.engine.processor.ranking.SingletonRanking;
import eu.smartdatalake.simsearch.engine.processor.ranking.ThresholdRanking;
import eu.smartdatalake.simsearch.engine.processor.ranking.randomaccess.CategoricalValueFinder;
import eu.smartdatalake.simsearch.engine.processor.ranking.randomaccess.NumericalValueFinder;
import eu.smartdatalake.simsearch.engine.processor.ranking.randomaccess.SpatialValueFinder;
import eu.smartdatalake.simsearch.engine.weights.Validator;
import eu.smartdatalake.simsearch.manager.DataSource;
import eu.smartdatalake.simsearch.manager.DatasetIdentifier;
import eu.smartdatalake.simsearch.manager.ingested.Index;
import eu.smartdatalake.simsearch.manager.ingested.categorical.TokenSet;
import eu.smartdatalake.simsearch.manager.ingested.categorical.TokenSetCollection;
import eu.smartdatalake.simsearch.manager.ingested.categorical.TokenSetCollectionReader;
import eu.smartdatalake.simsearch.manager.ingested.numerical.BPlusTree;
import eu.smartdatalake.simsearch.manager.ingested.numerical.INormal;
import eu.smartdatalake.simsearch.manager.ingested.spatial.Location;
import eu.smartdatalake.simsearch.manager.ingested.spatial.RTree;
import eu.smartdatalake.simsearch.manager.insitu.HttpRestConnector;
import eu.smartdatalake.simsearch.manager.insitu.JdbcConnector;
import eu.smartdatalake.simsearch.request.SearchRequest;
import eu.smartdatalake.simsearch.request.SearchSpecs;

/**
 * Handles a new multi-attribute similarity search request.
 * It applies one of the rank aggregation algorithms for its evaluation. Pivot-based similarity search is handled by another class.
 * A new instance of this class is invoked by the coordinator per search request.
 */
public class SearchHandler {

	Logger log = null;
	Assistant myAssistant;
	
	Map<String, Thread> tasks;
	Map<String, ISimilarity> similarities;
	Map<String, Double[]> weights;
	Map<String, INormal> normalizations;
	Map<String, Map<?, ?>> datasets;
	Map<String, Map<?,?>> lookups;
	Map<String, DataSource> dataSources;
	Map<String, Index> indices;
	Map<String, DatasetIdentifier> datasetIdentifiers;
	Map<String, IValueFinder> valueFinders;
	
	// List of queues that collect results from each running task
	Map<String, RankedList> queues;
	
	// List of atomic booleans to control execution of the various threads
	Map<String, AtomicBoolean> runControl;

	private boolean collectQueryStats;
	
	/**
	 * Constructor
	 * @param dataSources  Dictionary of the available data sources.
	 * @param datasetIdentifiers  Dictionary of the attributes available for similarity search operations.
	 * @param datasets  Dictionary of the attribute datasets available for querying.
	 * @param indices  Dictionary of in-memory indices on attribute data built for similarity search operations.
	 * @param normalizations  Dictionary of normalizations applicable to numerical attribute data. 
	 * @param log  Handle to the log file for notifications and execution statistics.
	 */
	public SearchHandler(Map<String, DataSource> dataSources, Map<String, DatasetIdentifier> datasetIdentifiers, Map<String, Map<?, ?>> datasets, Map<String, Index> indices, Map<String, INormal> normalizations, Logger log) {
			
		this.normalizations = normalizations;
		this.datasets = datasets;
		this.dataSources = dataSources;
		this.datasetIdentifiers = datasetIdentifiers;
		this.indices = indices;
		this.log = log;	
		myAssistant = new Assistant();
		
	    // New instances of various constructs specifically for this request
		queues = new HashMap<String, RankedList>();
		tasks = new HashMap<String, Thread>();
		similarities = new HashMap<String, ISimilarity>();
		weights = new HashMap<String, Double[]>();
		lookups = new HashMap<String, Map<?,?>>();
		runControl = new HashMap<String, AtomicBoolean>();
		valueFinders = new HashMap<String, IValueFinder>();   // Specifically used for random access to attribute values
	}

	
	/**
	 * Finds the internal identifier used for the data of a given attribute.
	 * @param column   The name of the attribute.
	 * @return  The dataset identifier that can be queried.
	 */
	private DatasetIdentifier findIdentifier(String column) {
		
		for (Entry<String, DatasetIdentifier> entry : datasetIdentifiers.entrySet()) {
			// TODO: Represent adequately composite attributes as arrays with the names of their constituent columns
            // Only considering operations involved in rank aggregation, i.e., excluding attributes supported in pivot-based similarity search
			if (entry.getValue().getValueAttribute().equals(column) && (entry.getValue().isQueryable()) && (entry.getValue().getOperation() != Constants.PIVOT_BASED)) {
            	return entry.getValue();
            }
        }
	
		return null;
	}
	

	/**
	 * Provides the maximum value available in a numerical attribute.
	 * @param dbConnector  The JDBC connection specification to a DBMS (if applicable).
	 * @param httpRestConnector  The HTTP connection specification for accessing REST APIs (if applicable).
	 * @param id  The identifier of the attribute in a dataset.
	 * @return  The maximum numerical value available in the specified attribute.
	 */
	private Double getMaxNumValue(JdbcConnector dbConnector, HttpRestConnector httpRestConnector, DatasetIdentifier id) {
		
		if (dbConnector != null)  {			// Querying against a DBMS
			return (Double)dbConnector.findSingletonValue("SELECT max(" + id.getValueAttribute() + ") FROM " + id.getDatasetName() + ";");
		}
		else if (httpRestConnector != null) {   // Querying against a REST API
			// FIXME: Custom retrieval of max value specifically from Elasticsearch
			String query = "{\"_source\": [\"" + id.getValueAttribute() + "\"]," + "\"query\": {\"match_all\": {}}, \"sort\": [{\"" + id.getValueAttribute() + "\": {\"order\": \"desc\"}}],  \"size\": 1}";
			return (Double)httpRestConnector.findSingletonValue(query);
//			return Double.MAX_VALUE;
		}
		else {				// Querying against in-memory indices over a CSV file
			BPlusTree<Double, String> index = (BPlusTree<Double, String>) indices.get(id.getHashKey());
			return index.calcMaxKey();
		}
	}

	
	/**
	 * Calculates the exact similarity score for a given entity with random access to the underlying datasets.
	 * @param id  The entity identifier.
	 * @param w  The identifier of the weight combination to be applied on the scores.
	 * @return  The exact aggregated score based on all attribute values.
	 */
	private double getExactScoreRandomAccess(String id, int w) {
		
		double sumWeights = 0.0;
		
		double score = 0.0;
		for (String task : tasks.keySet()) {	
			// Need to perform random access for all attributes
			Object val = this.lookups.get(task).get(id);   // Access attribute value from the lookup table
			// If this value is not available, then its retrieval from the DBMS or REST API also updates the in-memory data look-up
			if ((val == null) && ((this.datasetIdentifiers.get(task).getDataSource().getJdbcConnPool() != null) || ((this.datasetIdentifiers.get(task).getDataSource().getHttpConn() != null) && (!this.datasetIdentifiers.get(task).getDataSource().isSimSearchService())))) {
				val = valueFinders.get(this.datasetIdentifiers.get(task).getHashKey()).find(this.lookups.get(task), id);
			}
			//CAUTION! Weighted scores used in aggregation
			if (val != null) {   // If value is not null, then update score accordingly
				if (this.normalizations.get(task) != null)   // Apply normalization, if specified
					score += this.weights.get(task)[w] * this.similarities.get(task).calc(this.normalizations.get(task).normalize(val));
				else
					score += this.weights.get(task)[w] * this.similarities.get(task).calc(val);
			}
			sumWeights += this.weights.get(task)[w];
		}
		return (score / sumWeights);  // Weighted aggregate score over all running tasks (one per queried attribute)
	}
	
	
	/**
	 * Given a user-specified JSON configuration, execute the various similarity search queries and provide the ranked aggregated results.
	 * @param params   JSON configuration that provides the multi-attribute query specifications.
	 * @param query_timeout  Max execution time (in milliseconds) for ranking in a submitted query.
	 * @return   A JSON-formatted response with the ranked results.
	 */
	public SearchResponse[] search(SearchRequest params, long query_timeout) {
		
		long duration;
		SearchResponse[] responses;
		
		String notification = "";  // Any extra notification(s) to the final response
		
		IRankAggregator aggregator = null;
		String rankingMethod = Constants.DEFAULT_METHOD;   	// Threshold is the default ranking algorithm

		// Specifications for output CSV file or standard output (if applicable)
		OutputWriter outWriter = new OutputWriter(params.output);
		// Extra columns (not involved in similarity criteria) to report in the output
		String[] extraColumns = null;
		if ((params.output != null) && (params.output.extra_columns != null))
			extraColumns = params.output.extra_columns;
	
		int topk = 0;   // top-k value for ranked aggregated results
		    
		SearchSpecs[] queries = params.queries; 			// Array of specified queries
		
		// Construct for validating weights
		Validator weightValidator = new Validator();
		
		try {
			topk = params.k;
			// Allow only positive integers for top-k parameter
			if (topk < 1)
				throw new NumberFormatException();
			
			// Determine the ranking algorithm to be used for top-k aggregation
			if (params.algorithm != null)
				rankingMethod = params.algorithm;
			
		} catch(NumberFormatException e) {
			e.printStackTrace();
			responses = new SearchResponse[1];
			SearchResponse response = new SearchResponse();
			String msg = "Please specify a positive integer value for k. Search request aborted.";
			if ((params.output.format != null) && (params.output.format.equals("console")))
				System.out.println("NOTICE: "+ msg);
			log.writeln("Search request aborted due to illegal specification of parameters.");
			response.setNotification(msg);
			responses[0] = response;
			return responses;
		}
		
		// JDBC connections opened during this request
		List<JdbcConnector> openJdbcConnections = new ArrayList<JdbcConnector>();
		
		// HTTP connections opened during this request
		List<HttpRestConnector> openHttpConnections = new ArrayList<HttpRestConnector>();
		
	    // Iterate over the specified queries
		if (queries != null) {
			
			// Check for excessive top-k value
			if ((queries.length > 1) && (topk > Constants.K_MAX)) {
				responses = new SearchResponse[1];
				SearchResponse response = new SearchResponse();
				String msg = "Request aborted because no more than top-" + Constants.K_MAX + " results can be returned per query.";
				log.writeln(msg);
				if ((params.output.format != null) && (params.output.format.equals("console")))
					System.out.println("NOTICE: "+ msg);
				response.setNotification("Please specify a positive integer value up to " + Constants.K_MAX + " for k and submit your request again.");
				responses[0] = response;
				return responses;
			}

			// Instantiate a parser for the various types of query values
			QueryValueParser valParser = new QueryValueParser();
			boolean unusedFilter = false;   // Notify on any extra boolean filters non applicable to CSV data sources
			
	        for (SearchSpecs queryConfig: queries) {
	        	// Search column; Multiple attributes (e.g., lon, lat) will be combined into a virtual column [lon, lat] for searching
				String colValueName = queryConfig.column.toString();
				//DatasetIdentifier to be used for all constructs built for this attribute
				DatasetIdentifier id = findIdentifier(colValueName);				
				if (id == null) {
					String msg = "No dataset with attribute " + colValueName + " is available for search.";
					if ((params.output.format != null) && (params.output.format.equals("console")))
						System.out.println("NOTICE: "+ msg);
					notification.concat(msg);
					log.writeln(msg);
					throw new NullPointerException(msg);
				}
				
				String colKeyName = id.getKeyAttribute();
					
				// Get the respective connection details to this dataset
				DataSource dataSource = id.getDataSource();
				JdbcConnector jdbcConn = null;
				if (dataSource.getJdbcConnPool() != null) {  // Initialize a new JDBC connection from the pool
					jdbcConn = dataSource.getJdbcConnPool().initDbConnector();
					openJdbcConnections.add(jdbcConn);
				}

				HttpRestConnector httpConn = null;
				if (dataSource.getHttpConn() != null) {  // Initialize a new HTTP connection to the specified REST API
					httpConn = dataSource.getHttpConn();
					if (rankingMethod.equals("threshold")) {
//						rankingMethod = "partial_random_access";   // FIXME: Random access cannot be applied against the SimSearch REST API						
						responses = new SearchResponse[1];
						SearchResponse response = new SearchResponse();
						String msg = "Request aborted because random access is not supported against the SimSearch REST API. Please specify another ranking method.";
						log.writeln(msg);
						if ((params.output.format != null) && (params.output.format.equals("console")))
							System.out.println("NOTICE: "+ msg);
						response.setNotification("SimSearch REST API does not allow random access to the data. Please specify another ranking method, either partial_random_access or no_random_access. This request will be aborted.");
						responses[0] = response;
						return responses;						
					}
					httpConn.openConnection();
					openHttpConnections.add(httpConn);	
				}
				
				// operation
				String operation = myAssistant.decodeOperation(id.getOperation());
				
				// Assign a name to this operation/thread
				String name = operation + "." + id.getValueAttribute();
				
				// Inflate top-k value in order to fetch enough candidates from attribute search
				// In case just one attribute is involved, there is no need for inflation
				int collectionSize = (queries.length > 1) ? Constants.INFLATION_FACTOR * topk : topk;

				// Exponential decay factor lambda to be used in similarity calculations
				double decay = Constants.DECAY_FACTOR;
				if (queryConfig.decay != null)
					decay = queryConfig.decay;
				
				// Similarity measure: By default, decayed similarity will be used in all types of search
				ISimilarity<?> simMeasure = null;
			
				// Ranked list: a priority queue that will be used to collect query results
				RankedList resultsQueue = new RankedList();
				
				// WEIGHT: Array of specified weights to apply on a specific attribute
				if ((queryConfig.weights != null) && (queryConfig.weights.length == 0)) {  // Empty array of weights
					queryConfig.weights = null;
				} else if ((queryConfig.weights != null) && !weightValidator.check(colValueName, queryConfig.weights)) {
					responses = new SearchResponse[1];
					SearchResponse response = new SearchResponse();
					String msg = "Request aborted because at least one weight value for attribute " + colValueName + " is invalid.";
					log.writeln(msg);
					if ((params.output.format != null) && (params.output.format.equals("console")))
						System.out.println("NOTICE: "+ msg);
					response.setNotification(msg + " Weight values must be real numbers strictly between 0 and 1.");
					responses[0] = response;
					return responses;
				}
				weights.put(id.getHashKey(), queryConfig.weights);
				
				// SCALE factor to apply in normalizing distances
				// If not user-specified, 0 indicates it will be dynamically set from the results
				Double scale = (queryConfig.scale != null) ? queryConfig.scale : 0.0;
				
				// DatasetIdentifier to be used for all constructs built for this attribute
				// This is normally added at indexing stage; should be added here only when querying against a DBMS
				// On-the-fly look-up also occurs when the Partial Random Access method is applied
				if (!datasets.containsKey(id.getHashKey()) || (rankingMethod.equals("partial_random_access")))  {
					// Use a generated hash key of the column as a reference of values to be looked up for this attribute
					lookups.put(id.getHashKey(), new HashMap<String, Object>());
//					log.writeln("Look-up table for " + id.getValueAttribute() + " will be created on-the-fly during ranking.");
				}
				else {  // Otherwise, the lookup is the original collection of attribute values
					lookups.put(id.getHashKey(), datasets.get(id.getHashKey()));
				}
					
				// Settings for top-k similarity search on categorical values
				// Optional filters on datasets from JDBC or REST API sources can be specified; NOT allowed on ingested data from CSV files
				if (operation.equalsIgnoreCase("categorical_topk")) {
					
					Thread threadCatSearch = null;
					
					// QUERY SPECIFICATION
					// Search keywords can be specified either as a JSON array of string values or a concatenated string using the default delimiter
					String[] searchKeywords = (String[]) valParser.parse(queryConfig.value);
					if ((searchKeywords == null) || (searchKeywords.length == 0)) {
						reportValueError(id, String.valueOf(queryConfig.value), notification);
						continue;
					}
					
					TokenSetCollectionReader reader = new TokenSetCollectionReader();
					TokenSetCollection queryCollection = null;
					
					// This will create a collection for the query only; do NOT use qgrams (qgram = 0)
					queryCollection = reader.createFromQueryKeywords(searchKeywords, 0, log);

					// FIXME: If user has not specified a scale factor, do NOT apply scaling on Jaccard distances 
//					scale = (scale > 0.0) ? scale : 1.0;
					
					// Jaccard distance is applied on categorical (textual) values
					// Similarity also indicates the corresponding task serial number
					simMeasure = new DecayedSimilarity(new CategoricalDistance(queryCollection.sets.get("querySet")), decay, scale, tasks.size());
					similarities.put(id.getHashKey(), simMeasure);
					
					// Create an instance of the categorical search query (CATEGORICAL_TOPK = 0)
					if (jdbcConn != null)  {		// Querying against a DBMS
						id.setOperation(Constants.CATEGORICAL_TOPK);
						// FIXME: Separator for search keywords must be ";" in this case
						SimSearchJdbcQuery catSearch = new SimSearchJdbcQuery(jdbcConn, Constants.CATEGORICAL_TOPK, id.getDatasetName(), queryConfig.filter, colKeyName, colValueName, String.join(";", searchKeywords), topk, collectionSize, simMeasure, resultsQueue, lookups, id.getHashKey(), log);
						valueFinders.put(id.getHashKey(), new CategoricalValueFinder(jdbcConn, catSearch.sqlSingleValueRetrievalTemplate));
						threadCatSearch = new Thread(catSearch);
						runControl.put(id.getHashKey(), catSearch.running);
					}
					else if (httpConn != null) {	// Querying against a REST API
						if (dataSource.isSimSearchService()) {   // This is an instance of another SimSearch REST API
							SimSearchRestQuery catSearch = new SimSearchRestQuery(httpConn, Constants.CATEGORICAL_TOPK, colKeyName, colValueName, String.join(",", searchKeywords), collectionSize, simMeasure, resultsQueue, lookups, id.getHashKey(), log);
							valueFinders.put(id.getHashKey(), new CategoricalValueFinder(httpConn, null));  // By default, random access is prohibited
							threadCatSearch = new Thread(catSearch);
							runControl.put(id.getHashKey(), catSearch.running);
						}
						else {  // This is an ElasticSearch REST API
							ElasticSearchRestQuery catSearch = new ElasticSearchRestQuery(httpConn, Constants.CATEGORICAL_TOPK, queryConfig.filter, colKeyName, colValueName, String.join(",", searchKeywords), topk, collectionSize, simMeasure, resultsQueue, lookups, id.getHashKey(), log);
							valueFinders.put(id.getHashKey(), new CategoricalValueFinder(httpConn, catSearch.queryValueRetrievalTemplate));
							threadCatSearch = new Thread(catSearch);
							runControl.put(id.getHashKey(), catSearch.running);
						}
					}
					else {			// Querying against in-memory indices over a CSV file
						IndexSimSearch catSearch = new IndexSimSearch(Constants.CATEGORICAL_TOPK, name, indices.get(id.getHashKey()), datasets.get(id.getHashKey()), queryCollection, topk, collectionSize, simMeasure, resultsQueue, id.getHashKey(), log);
						threadCatSearch = new Thread(catSearch);
						runControl.put(id.getHashKey(), catSearch.running);
						// Extra boolean filters not supported over CSV data sources
			        	unusedFilter = unusedFilter || (queryConfig.filter != null); 
					}
					
					threadCatSearch.setName(name);
					tasks.put(id.getHashKey(), threadCatSearch);
					queues.put(id.getHashKey(), resultsQueue);
				}
				// settings for top-k similarity search on numerical values
				else if (operation.equalsIgnoreCase("numerical_topk")) {
					
					Thread threadNumSearch = null;
					
					// QUERY SPECIFICATION
					String val = String.valueOf(queryConfig.value);
					Double searchingKey;
					try {
						// In case the "max" value is specified in search, identify this numerical value in the dataset
						if (val.equalsIgnoreCase("max"))
							searchingKey = getMaxNumValue(jdbcConn, httpConn, id);
						else   // Otherwise, get the user-specified numerical value to search
							searchingKey = Double.parseDouble(val);
					}
					catch(Exception e) {
						searchingKey = null;
					}
					// Check for NULL or invalid numerical value
					if ((searchingKey == null) || (searchingKey.isNaN())) {
						reportValueError(id, val, notification);
						continue;
					}
					
//					System.out.println("Numerical value: " + searchingKey);
					
					// Check whether data has been normalized during indexing
					INormal normal = normalizations.get(id.getHashKey());

					// The search key must be also normalized as was the input dataset
					if (normal != null) {
						searchingKey = normal.normalize(searchingKey);
						log.writeln("Normalized search key:" + searchingKey);
					}

					// Absolute difference is used for estimating distance (and thus, similarity) of numerical values
					// Similarity also indicates the corresponding task serial number
					simMeasure = new DecayedSimilarity(new NumericalDistance(searchingKey), decay, scale, tasks.size());
					similarities.put(id.getHashKey(), simMeasure);
					
					// Create an instance of the numerical search query (NUMERICAL_TOPK = 2)
					if (jdbcConn != null)  {		// Querying against a DBMS
						id.setOperation(Constants.NUMERICAL_TOPK);
						SimSearchJdbcQuery numSearch = new SimSearchJdbcQuery(jdbcConn, Constants.NUMERICAL_TOPK, id.getDatasetName(), queryConfig.filter, colKeyName, colValueName, String.valueOf(searchingKey), topk, collectionSize, simMeasure, resultsQueue, lookups, id.getHashKey(), log);
						valueFinders.put(id.getHashKey(), new NumericalValueFinder(jdbcConn, numSearch.sqlSingleValueRetrievalTemplate));
						threadNumSearch = new Thread(numSearch);
						runControl.put(id.getHashKey(), numSearch.running);	
					}
					else if (httpConn != null) {	// Querying against a REST API
						if (dataSource.isSimSearchService()) {   // This is an instance of another SimSearch REST API
							SimSearchRestQuery numSearch = new SimSearchRestQuery(httpConn, Constants.NUMERICAL_TOPK, colKeyName, colValueName, String.valueOf(searchingKey), collectionSize, simMeasure, resultsQueue, lookups, id.getHashKey(), log);
							valueFinders.put(id.getHashKey(), new NumericalValueFinder(httpConn, null));  // By default, random access is prohibited
							threadNumSearch = new Thread(numSearch);
							runControl.put(id.getHashKey(), numSearch.running);
						}
						else {  // This is an ElasticSearch REST API
							ElasticSearchRestQuery numSearch = new ElasticSearchRestQuery(httpConn, Constants.NUMERICAL_TOPK, queryConfig.filter, colKeyName, colValueName, String.valueOf(searchingKey), topk, collectionSize, simMeasure, resultsQueue, lookups, id.getHashKey(), log);
							valueFinders.put(id.getHashKey(), new NumericalValueFinder(httpConn, numSearch.queryValueRetrievalTemplate));
							threadNumSearch = new Thread(numSearch);
							runControl.put(id.getHashKey(), numSearch.running);
						}	
					}
					else {		// Querying against in-memory indices over a CSV file						
						// Identify the B+-tree already built for this attribute
						BPlusTree<Double, String> index = (BPlusTree<Double, String>) indices.get(id.getHashKey());
						// collectionSize = -1 -> no prefixed bound on the number of results to fetch from numerical similarity search
						IndexSimSearch numSearch = new IndexSimSearch(Constants.NUMERICAL_TOPK, name, index, searchingKey, topk, collectionSize, simMeasure, resultsQueue, id.getHashKey(), log);
						threadNumSearch = new Thread(numSearch);
						runControl.put(id.getHashKey(), numSearch.running);		
						// Extra boolean filters not supported over CSV data sources
			        	unusedFilter = unusedFilter || (queryConfig.filter != null); 
					}
					
					threadNumSearch.setName(name);
					tasks.put(id.getHashKey(), threadNumSearch);
					queues.put(id.getHashKey(), resultsQueue);
				}
				// settings for k-NN similarity search on spatial locations
				else if (operation.equalsIgnoreCase("spatial_knn")) {
					
					Thread threadGeoSearch = null;
					
					// QUERY SPECIFICATION
					// Expecting a WKT representation of a query location
					Geometry queryPoint = valParser.parseGeometry(queryConfig.value);
					// Check for NULL query value
					if (queryPoint == null) {
						reportValueError(id, String.valueOf(queryConfig.value), notification);
						continue;
					}
					Location queryLocation = new Location("QUERY", queryPoint);
					
					// By default, Haversine distance is used in similarity calculations between spatial locations
					// Similarity also indicates the corresponding task serial number
					simMeasure = new DecayedSimilarity(new SpatialDistance(queryPoint), decay, scale, tasks.size());
					similarities.put(id.getHashKey(), simMeasure);
					
					// Create an instance of the spatial similarity search query (SPATIAL_KNN = 1)
					if (jdbcConn != null)  {		// Querying against a DBMS
						id.setOperation(Constants.SPATIAL_KNN);
						SimSearchJdbcQuery geoSearch = new SimSearchJdbcQuery(jdbcConn, Constants.SPATIAL_KNN, id.getDatasetName(), queryConfig.filter, colKeyName, colValueName, queryPoint.toText(), topk, collectionSize, simMeasure, resultsQueue, lookups, id.getHashKey(), log);
						valueFinders.put(id.getHashKey(), new SpatialValueFinder(jdbcConn, geoSearch.sqlSingleValueRetrievalTemplate));
						threadGeoSearch = new Thread(geoSearch);
						runControl.put(id.getHashKey(), geoSearch.running);
					}
					else if (httpConn != null) {	// Querying against a REST API
						if (dataSource.isSimSearchService()) {   // This is an instance of another SimSearch REST API
							SimSearchRestQuery geoSearch = new SimSearchRestQuery(httpConn, Constants.SPATIAL_KNN, colKeyName, colValueName, queryPoint.toText(), collectionSize, simMeasure, resultsQueue, lookups, id.getHashKey(), log);
							valueFinders.put(id.getHashKey(), new SpatialValueFinder(httpConn, null));  // By default, random access is prohibited
							threadGeoSearch = new Thread(geoSearch);
							runControl.put(id.getHashKey(), geoSearch.running);
						}
						else {  // This is an ElasticSearch REST API
							// FIXME: Geo-points in ElasticSearch are expressed as a string with the format: "lat, lon"
							ElasticSearchRestQuery geoSearch = new ElasticSearchRestQuery(httpConn, Constants.SPATIAL_KNN, queryConfig.filter, colKeyName, colValueName, "" + queryPoint.getCoordinates()[0].y + "," + queryPoint.getCoordinates()[0].x, topk, collectionSize, simMeasure, resultsQueue, lookups, id.getHashKey(), log);
							valueFinders.put(id.getHashKey(), new SpatialValueFinder(httpConn, geoSearch.queryValueRetrievalTemplate));
							threadGeoSearch = new Thread(geoSearch);
							runControl.put(id.getHashKey(), geoSearch.running);
						}
					}
					else {		// Querying against in-memory indices over a CSV file	
						// Identify the R-tree already built for this attribute
						RTree<String, Location> index = (RTree<String, Location>) indices.get(id.getHashKey());
						IndexSimSearch geoSearch = new IndexSimSearch(Constants.SPATIAL_KNN, name, index, queryLocation, topk, collectionSize, simMeasure, resultsQueue, id.getHashKey(), log);
						threadGeoSearch = new Thread(geoSearch);
						runControl.put(id.getHashKey(), geoSearch.running);
						// Extra boolean filters not supported over CSV data sources
			        	unusedFilter = unusedFilter || (queryConfig.filter != null); 
					}
					
					threadGeoSearch.setName(name);
					tasks.put(id.getHashKey(), threadGeoSearch);
					queues.put(id.getHashKey(), resultsQueue);		
				}
				// settings for top-k similarity search on date/time values
				else if (operation.equalsIgnoreCase("temporal_topk")) {
					
					Thread threadNumSearch = null;
					
					// QUERY SPECIFICATION
					// Parse the user-specified date/time value to a double number (epoch) for searching in the index
					// Epoch used only against ingested in-memory data
					Double searchingKey;
					if ((searchingKey = valParser.parseDate(queryConfig.value)) == null)
						searchingKey = (Double) valParser.parse(queryConfig.value);
					
					// Check for NULL or invalid temporal value
					if ((searchingKey == null) || (searchingKey.isNaN())) {
						reportValueError(id, String.valueOf(queryConfig.value), notification);
						continue;
					}
					
		        	// Check compatibility of data types
		        	if (id.getDatatype() != valParser.getDataType()) {
		        		String msg = "Query value " + String.valueOf(queryConfig.value) + " is not of type " + id.getDatatype() + " as the attribute data.";
		        		log.writeln(msg);
						if ((params.output.format != null) && (params.output.format.equals("console")))
							System.out.println("NOTICE: "+ msg);
		        		notification.concat(msg);
		        	}
//					System.out.println("Epoch value: " + searchingKey);

					// Absolute difference is used for estimating distance (and thus, similarity) of epoch numerical values
					// Similarity also indicates the corresponding task serial number
					simMeasure = new DecayedSimilarity(new NumericalDistance(searchingKey), decay, scale, tasks.size());
					similarities.put(id.getHashKey(), simMeasure);
					
					// Employ a temporal search query (TEMPORAL_TOPK = 7)
					if (jdbcConn != null)  {		// Querying against a DBMS using the original date/time value
						id.setOperation(Constants.TEMPORAL_TOPK);
						SimSearchJdbcQuery numSearch = new SimSearchJdbcQuery(jdbcConn, Constants.TEMPORAL_TOPK, id.getDatasetName(), queryConfig.filter, colKeyName, colValueName, String.valueOf(queryConfig.value), topk, collectionSize, simMeasure, resultsQueue, lookups, id.getHashKey(), log);
						valueFinders.put(id.getHashKey(), new NumericalValueFinder(jdbcConn, numSearch.sqlSingleValueRetrievalTemplate));
						threadNumSearch = new Thread(numSearch);
						runControl.put(id.getHashKey(), numSearch.running);	
					}
					else if (httpConn != null) {	// Querying against a REST API
						if (dataSource.isSimSearchService()) {   // This is an instance of another SimSearch REST API with the original date/time value
							SimSearchRestQuery numSearch = new SimSearchRestQuery(httpConn, Constants.TEMPORAL_TOPK, colKeyName, colValueName, String.valueOf(queryConfig.value), collectionSize, simMeasure, resultsQueue, lookups, id.getHashKey(), log);
							valueFinders.put(id.getHashKey(), new NumericalValueFinder(httpConn, null));  // By default, random access is prohibited
							threadNumSearch = new Thread(numSearch);
							runControl.put(id.getHashKey(), numSearch.running);
						}
						else {  // Querying against an ElasticSearch REST API using the original date/time value
							// TODO: Check that Elasticsearch supports search over date/time values
							ElasticSearchRestQuery numSearch = new ElasticSearchRestQuery(httpConn, Constants.TEMPORAL_TOPK, queryConfig.filter, colKeyName, colValueName, String.valueOf(queryConfig.value), topk, collectionSize, simMeasure, resultsQueue, lookups, id.getHashKey(), log);
							valueFinders.put(id.getHashKey(), new NumericalValueFinder(httpConn, numSearch.queryValueRetrievalTemplate));
							threadNumSearch = new Thread(numSearch);
							runControl.put(id.getHashKey(), numSearch.running);
						}	
					}
					else {		// Querying with the epoch value against in-memory indices over a CSV file	
						// A numerical search query is used internally for ingested temporal data
						// Identify the B+-tree already built for this attribute
						BPlusTree<Double, String> index = (BPlusTree<Double, String>) indices.get(id.getHashKey());
						// collectionSize = -1 -> no prefixed bound on the number of results to fetch from numerical similarity search
						IndexSimSearch numSearch = new IndexSimSearch(Constants.TEMPORAL_TOPK, name, index, searchingKey, topk, collectionSize, simMeasure, resultsQueue, id.getHashKey(), log);
						threadNumSearch = new Thread(numSearch);
						runControl.put(id.getHashKey(), numSearch.running);		
						// Extra boolean filters not supported over CSV data sources
			        	unusedFilter = unusedFilter || (queryConfig.filter != null); 
					}
					
					threadNumSearch.setName(name);
					tasks.put(id.getHashKey(), threadNumSearch);
					queues.put(id.getHashKey(), resultsQueue);
				}
				// settings for top-k textual (string) similarity search
				else if (operation.equalsIgnoreCase("textual_topk")) {
					
					Thread threadStringSearch = null;
					
					// QUERY SPECIFICATION
					// Search string
					String[] parsedValue = (String[]) valParser.parse(queryConfig.value);  // returns an array
					if ((parsedValue == null) || (parsedValue.length == 0)) {
						reportValueError(id, String.valueOf(queryConfig.value), notification);
						continue;
					}
					String searchString = parsedValue[0];
					
					// Identify q-gram used in original dataset
					int qgram = Constants.QGRAM;					
					if (!id.getDataSource().isInSitu())   // Auto-detect QGRAM value for ingested data only
						qgram = ((TokenSet) datasets.get(id.getHashKey()).values().iterator().next()).tokens.get(0).length();
					
					// Create a collection of q-gram representations for the query only
					TokenSetCollectionReader reader = new TokenSetCollectionReader();
					TokenSetCollection queryCollection = reader.createFromQueryString(searchString, qgram, log);

//					System.out.println(searchString + " qgram:" + qgram + " tokens:" +  queryCollection.sets.values().iterator().next().tokens.toArray());
					
					// FIXME: If user has not specified a scale factor, do NOT apply scaling on Jaccard distances 
//					scale = (scale > 0.0) ? scale : 1.0;
					
					// Jaccard distance is applied on categorical (textual) values
					// Similarity also indicates the corresponding task serial number
					simMeasure = new DecayedSimilarity(new CategoricalDistance(queryCollection.sets.get("querySet")), decay, scale, tasks.size());
					similarities.put(id.getHashKey(), simMeasure);
					
					// Create an instance of the textual (string) search query (TEXTUAL_TOPK = 8)
					if (jdbcConn != null)  {		// Querying against a DBMS
						id.setOperation(Constants.TEXTUAL_TOPK);
						// FIXME: Separator for search keywords must be ";" in this case
						SimSearchJdbcQuery stringSearch = new SimSearchJdbcQuery(jdbcConn, Constants.TEXTUAL_TOPK, id.getDatasetName(), queryConfig.filter, colKeyName, colValueName, searchString, topk, collectionSize, simMeasure, resultsQueue, lookups, id.getHashKey(), log);
						valueFinders.put(id.getHashKey(), new CategoricalValueFinder(jdbcConn, stringSearch.sqlSingleValueRetrievalTemplate));
						threadStringSearch = new Thread(stringSearch);
						runControl.put(id.getHashKey(), stringSearch.running);
					}
					else if (httpConn != null) {	// Querying against a REST API
						if (dataSource.isSimSearchService()) {   // This is an instance of another SimSearch REST API
							SimSearchRestQuery stringSearch = new SimSearchRestQuery(httpConn, Constants.TEXTUAL_TOPK, colKeyName, colValueName, searchString, collectionSize, simMeasure, resultsQueue, lookups, id.getHashKey(), log);
							valueFinders.put(id.getHashKey(), new CategoricalValueFinder(httpConn, null));  // By default, random access is prohibited
							threadStringSearch = new Thread(stringSearch);
							runControl.put(id.getHashKey(), stringSearch.running);
						}
						else {  // This is an ElasticSearch REST API
							ElasticSearchRestQuery stringSearch = new ElasticSearchRestQuery(httpConn, Constants.TEXTUAL_TOPK, queryConfig.filter, colKeyName, colValueName, searchString, topk, collectionSize, simMeasure, resultsQueue, lookups, id.getHashKey(), log);
							valueFinders.put(id.getHashKey(), new CategoricalValueFinder(httpConn, stringSearch.queryValueRetrievalTemplate));
							threadStringSearch = new Thread(stringSearch);
							runControl.put(id.getHashKey(), stringSearch.running);
						}
					}
					else {			// Querying against in-memory indices over a CSV file
						IndexSimSearch stringSearch = new IndexSimSearch(Constants.TEXTUAL_TOPK, name, indices.get(id.getHashKey()), datasets.get(id.getHashKey()), queryCollection, topk, collectionSize, simMeasure, resultsQueue, id.getHashKey(), log);
						threadStringSearch = new Thread(stringSearch);
						runControl.put(id.getHashKey(), stringSearch.running);
						// Extra boolean filters not supported over CSV data sources
			        	unusedFilter = unusedFilter || (queryConfig.filter != null);  
					}
					
					threadStringSearch.setName(name);
					tasks.put(id.getHashKey(), threadStringSearch);
					queues.put(id.getHashKey(), resultsQueue);
				}
				// TODO: Include other types of operations...
				else {
					log.writeln("Unknown operation specified: " + operation);
				}			
			}
	        // Notify if any filters were specified for CSV data sources; these cannot be applied
			if (unusedFilter) {
				String msg = "Unsupported boolean filters specified in this query over ingested data will be ignored.";
				log.writeln(msg);
				if ((params.output.format != null) && (params.output.format.equals("console")))
					System.out.println("NOTICE: " + msg);
			}
		}


		// Start all tasks; each query will now start fetching results
		for (Entry<String, Thread> task: tasks.entrySet()) {
			task.getValue().start();
		}
	
		// Perform the ranked aggregation process
		duration = System.nanoTime();

		if (queries.length > 1) {
			// Instantiate the rank aggregator that will handle results from the various threads
			// Execute rank aggregation separately for all combination of weights
			switch(rankingMethod){
			case "no_random_access":
				aggregator = new NoRandomAccessRanking(datasetIdentifiers, lookups, similarities, weights, normalizations, tasks, queues, runControl, topk, log);
				break;
			case "partial_random_access":
				aggregator = new PartialRandomAccessRanking(datasetIdentifiers, lookups, similarities, weights, normalizations, tasks, queues, runControl, topk, log);
				break;
			case "threshold":   // This is the default method, if not explicitly specified by the user
				aggregator = new ThresholdRanking(datasetIdentifiers, lookups, similarities, weights, normalizations, tasks, queues, valueFinders, runControl, topk, log);
				break;
			default:
				responses = new SearchResponse[1];
				SearchResponse response = new SearchResponse();
				String msg = "No ranking method specified or the one specified is not applicable on the available data sources!";
				log.writeln(msg);
				if ((params.output.format != null) && (params.output.format.equals("console")))
					System.out.println("NOTICE: "+ msg);
				response.setNotification(msg + " Weight values must be real numbers strictly between 0 and 1.");
				responses[0] = response;
				return responses;
			}
		}
		else {   // Only one attribute is involved, so no rank aggregation needs to be executed
			aggregator = new SingletonRanking(datasetIdentifiers, similarities, weights, tasks, queues, runControl, topk, log);
		}
		
		responses = null;
			
		// Collect results that may be issued as JSON
		IResult[][] results = aggregator.proc(query_timeout);
		
	    // USED FOR EXPERIMENTS ONLY: Change the estimated aggregate scores with the exact ones
	    if (isCollectQueryStats() && (!rankingMethod.equals("threshold"))) {
	    	for (int w = 0; w < weights.entrySet().iterator().next().getValue().length; w++) {
		    	for (int p = 0; p < results[w].length; p++) {
		    		// Apply random access to original attribute values and replace estimated score with the exact one
		    		results[w][p].setScore(getExactScoreRandomAccess(results[w][p].getId(), w));	
		    	}
	    	}
	    }
	    
		duration = System.nanoTime() - duration;
		double execTime = duration / 1000000000.0;	
		
		// EXTRA COLUMNS: Collect values from in-situ data sources for any extra attributes required for the results
		if (extraColumns != null) {
			// First collect identifiers of all results
			Set<String> setResultId = new HashSet<String>();
			for (int w = 0; w < weights.entrySet().iterator().next().getValue().length; w++) {
				for (int i = 0; i < results[w].length; i++) {
					setResultId.add(results[w][i].getId());
				}
			}
			
			// For each extra attribute, check whether an in-situ query must be submitted to retrieve values for the result identifiers 
			for (String col: extraColumns) {
				//DatasetIdentifier to be used for all constructs built for this attribute
				DatasetIdentifier id = findIdentifier(col);				
				if (id == null) {
					String msg = "No dataset with attribute " + col + " is available for search.";
					if ((params.output.format != null) && (params.output.format.equals("console")))
						System.out.println("NOTICE: "+ msg);
					notification.concat(msg);
					log.writeln(msg);
					continue;
				}

				// No need for extra look up if attribute data is already ingested in memory
				if (datasets.containsKey(id.getHashKey()))
					continue;
					
				// Otherwise, create a lookup of values for this extra attribute to be reported in the final results
				lookups.put(id.getHashKey(), new HashMap<String, Object>());
					
				// The name of the identifier column in this source
				String colKeyName = id.getKeyAttribute();
				// operation
				String operation = myAssistant.decodeOperation(id.getOperation());
					
				// Get the respective connection details to this dataset
				DataSource dataSource = id.getDataSource();
				// Initialize a new JDBC connection from the pool
				if (dataSource.getJdbcConnPool() != null) {  
					JdbcConnector jdbcConn = dataSource.getJdbcConnPool().initDbConnector();
					openJdbcConnections.add(jdbcConn);
					// Run a search query against the DBMS to get the attribute values on the specific object identifiers
					SimSearchJdbcQuery lookupSearch = new SimSearchJdbcQuery(jdbcConn, id.getOperation(), id.getDatasetName(), null, colKeyName, col, "", topk, 0, null, null, lookups, id.getHashKey(), log);
					lookupSearch.appendValues(setResultId);   // Append the retrieved values to the lookup on-the-fly
				}
				else if (dataSource.getHttpConn() != null) {  // Initialize a new HTTP connection to the specified REST API
					HttpRestConnector httpConn = dataSource.getHttpConn();
					if (httpConn.isSimSearchInstance()) {
						String msg = "SimSearch API does not support random access to the data. No values can be collected for attribute " + col + ".";
						log.writeln(msg);
						if ((params.output.format != null) && (params.output.format.equals("console")))
							System.out.println("NOTICE: "+ msg);
						continue;
					}
					httpConn.openConnection();
					openHttpConnections.add(httpConn);	
					
					// Run a search query against the REST API to get the attribute values on the specific object identifiers
					ElasticSearchRestQuery lookupSearch = new ElasticSearchRestQuery(httpConn, id.getOperation(), null, colKeyName, col, "", topk, 0, null, null, lookups, id.getHashKey(), log);
					lookupSearch.appendValues(setResultId);   // Append the retrieved values to the lookup on-the-fly		
				}
			}
		}
	
		// Format response
		SearchResponseFormat responseFormat = new SearchResponseFormat();
		responses = responseFormat.proc(results, extraColumns, weights, datasetIdentifiers, datasets, lookups, similarities, normalizations, null, topk, this.isCollectQueryStats(), notification, execTime, outWriter);
		log.writeln("SimSearch [" + rankingMethod + "] issued " + responses[0].getRankedResults().length + " results. Processing time: " + execTime + " sec.");

		// Close output writer to CSV (if applicable)
		if (outWriter.isSet())
			outWriter.close();
		
		// Execution cost for experimental results; the same time cost concerns all weight combinations
		for (SearchResponse response: responses) { 
			response.setTimeInSeconds(execTime);	
		}
		
		// Close any JDBC connections
		for (JdbcConnector jdbcConn: openJdbcConnections)
			jdbcConn.closeConnection();

		// Close any HTTP connections
		for (HttpRestConnector httpConn: openHttpConnections)
			httpConn.closeConnection();
		
		// Response can be formatted as JSON
		return responses;
	}


	/**
	 * Indicates whether the platform is empirically evaluated and collects execution statistics.
	 * @return  True, if collecting execution statistics; otherwise, False.
	 */
	public boolean isCollectQueryStats() {
		return collectQueryStats;
	}


	/**
	 * Specifies whether the platform will be collecting execution statistics when running performance tests. 
	 * @param collectQueryStats  True, if collecting execution statistics; otherwise, False.
	 */
	public void setCollectQueryStats(boolean collectQueryStats) {
		this.collectQueryStats = collectQueryStats;
	}
	
	
	/**
	 * Reports an error regarding the search value specified for a particular query operation.
	 * @param id  The identifier of the dataset targeted by the serach value.
	 * @param val  The search value specified in the query.
	 * @param notification  The notification string to update with a message.
	 */
	private void reportValueError(DatasetIdentifier id, String val, String notification) {
		
		String msg = "Query value: " + val + ". Operation " + myAssistant.decodeOperation(id.getOperation()) + " using a NULL or invalid search value is not supported! This attribute will be ignored in search.";
		log.writeln(msg);
		notification.concat(msg);
		// No longer consider weights on this attribute
		weights.remove(id.getHashKey());
	}
}
