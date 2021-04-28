package eu.smartdatalake.simsearch.engine.processor.insitu;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.locationtech.jts.io.WKTReader;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

import eu.smartdatalake.simsearch.Assistant;
import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.engine.IDistance;
import eu.smartdatalake.simsearch.engine.measure.ISimilarity;
import eu.smartdatalake.simsearch.engine.processor.ISimSearch;
import eu.smartdatalake.simsearch.engine.processor.ranking.PartialResult;
import eu.smartdatalake.simsearch.engine.processor.ranking.RankedList;
import eu.smartdatalake.simsearch.manager.ingested.temporal.DateTimeParser;
import eu.smartdatalake.simsearch.manager.insitu.HttpRestConnector;


/**
 * Implements functionality for various similarity search queries (numerical, categorical, spatial) against an ElasticSearch REST API.
 * @param <K>  Type variable representing the keys of the stored objects (i.e., primary keys).
 * @param <V>  Type variable representing the values of the stored objects (i.e., their values on a given attribute).
 * 
 * CAUTION! A default limit of 10000 results per query is typically specified per index in ElastiSearch. This should be altered by the ES administrator.
 */
public class ElasticSearchRestQuery<K extends Comparable<? super K>, V> implements ISimSearch<K, V>, Runnable {

	Logger log = null;
	Assistant myAssistant;
	public AtomicBoolean running = new AtomicBoolean(false);
	
	URI uri = null;
    String query = null;			// Query to be composed for top-k search
	
	String valColumnName = null;
	ISimilarity simMeasure;
	RankedList resultsQueue;
	public int collectionSize;
	int topk;
	int operation;       		//Type of the search query
	String hashKey = null;   	// The unique hash key assigned to this search query
	
	HttpRestConnector httpConn = null;
	
	Map<String, HashMap<K,V>> datasets = null;
	
	// Compose the query template for value retrieval 
	public String queryValueRetrievalTemplate = null;	

	/**
	 * Constructor
	 * @param httpConn  The HTTP connection that provides access to the data.
	 * @param operation  The type of the similarity search query (0: CATEGORICAL_TOPK, 1: SPATIAL_KNN, 2: NUMERICAL_TOPK).
	 * @param filter  Optional filter in ES syntax to be applied on data prior to similarity search.
	 * @param keyColumnName  Name of the attribute holding the entity identifiers (keys).
	 * @param valColumnName  Name of the attribute containing numerical values of these entities.
	 * @param searchValue  String specifying the query value according to the type os the search operation (i.e., keywords, a location, or a number).
	 * @param topk  The number of the final top-k results.
	 * @param collectionSize  The count of candidate entities to fetch.
	 * @param simMeasure  The similarity measure to be used in the search.
	 * @param resultsQueue  Queue to collect query results.
	 * @param datasets  Dictionary of the attribute data available for search.
	 * @param hashKey  The unique hash key assigned to this search query.
	 * @param log  Handle to the log file for keeping messages and statistics.
	 */
	public ElasticSearchRestQuery(HttpRestConnector httpConn, int operation, String filter, String keyColumnName, String valColumnName, String searchValue, int topk, int collectionSize, ISimilarity simMeasure, RankedList resultsQueue, Map<String, HashMap<K,V>> datasets, String hashKey, Logger log) {
		  
		super();

		this.log = log;
		myAssistant = new Assistant();
		this.hashKey = hashKey;
		this.operation = operation;
		this.valColumnName = valColumnName;
		this.simMeasure = simMeasure;
		this.resultsQueue = resultsQueue;
		this.datasets = datasets;
		this.topk = topk;

		// Limit results returned per request up to a certain number
		// By default setting for the max number of returned results (if applied by the HTTP server)
		int maxSize = httpConn.getMaxResultCount();
		this.collectionSize = ((collectionSize > maxSize) ? maxSize : collectionSize); 
		
		this.httpConn = httpConn;
		
    	// Construct similarity search request according to the type of the operation
    	// FIXME: REST APIs may have different specifications for the various types of queries; currently using the ElasticSearch dialect
		// CAUTION! ElasticSearch is sensitive to scale and decay parameters; reduced decay values could miss several more relevant results 
		// Scale factors are expressed in the specific units depending on the attribute data type
		if (operation == Constants.NUMERICAL_TOPK) {
			query = "{\"function_score\": {\"query\": {\"exists\": { \"field\": \"" + valColumnName + "\" }},"
				+ "\"exp\": {\"" + valColumnName + "\": {\"origin\": \"" + searchValue 
				+ "\",\"scale\": \"1\",\"decay\" : 0.99999}}}}";
			}
		else if (operation == Constants.TEMPORAL_TOPK) {
			query = "{\"function_score\": {\"query\": {\"exists\": { \"field\": \"" + valColumnName + "\" }},"
				+ "\"exp\": {\"" + valColumnName + "\": {\"origin\": \"" + searchValue 
				+ "\",\"scale\": \"1d\",\"decay\" : 0.99999}}}}";
			}
		else if (operation == Constants.CATEGORICAL_TOPK) {
			query = "{ \"match\": { \"" + valColumnName + "\": \"" + searchValue + "\" } }";
			}
		else if (operation == Constants.TEXTUAL_TOPK) {
			query = "{ \"match\": { \"" + valColumnName + "\": \"" + searchValue + "\" } }";
			}
		else if (operation == Constants.SPATIAL_KNN) {
			query = "{\"function_score\": {\"query\": {\"exists\": { \"field\": \"" + valColumnName + "\" }},"
				+ "\"exp\": {\"" + valColumnName + "\": {\"origin\": \"" + searchValue 
				+ "\", \"scale\": \"100m\",\"decay\" : 0.99999}}}}";
			}
		
		// Extra user-specified filter context to be applied prior to similarity search
		if (filter != null) {
			query = "{\"bool\": {\"must\": [" + query + "], \"filter\": " + filter + "}}";
		}

		// Final search request to be submitted for evaluation
		query = "{\"_source\": [\"" + keyColumnName + "\", \"" + valColumnName + "\"], \"query\": " + query + ",\"size\": " + this.collectionSize + "}";
		//System.out.println(query);
		
		// Template of the query that retrieves the value for a particular object ($id is a placeholder for its identifier)
		queryValueRetrievalTemplate = "{\"_source\": [\"" + keyColumnName + "\", \"" + valColumnName + "\"]," + "\"query\": {\"ids\": {\"values\": [\"$id\"]}}}";
	}

	
	/**
	 * Connects to a REST API and retrieves items qualifying to the submitted similarity search request.
	 * @param M  The count of results to fetch.
	 * @param partialResults  The queue that collects candidate results obtained from the specified query.
	 * @return  The number of collected results.
	 */
    public int compute(int M, RankedList partialResults) {
    	
    	JSONParser jsonParser = new JSONParser();
    	int numMatches = 0;
    	WKTReader wktReader = new WKTReader();
    	long duration = System.nanoTime();	 
   
    	// Queue to collect results and keep them by ASCENDING distance as calculated by the REST API
    	ListMultimap<Double, PartialResult> resQueue = Multimaps.newListMultimap(new TreeMap<>(), ArrayList::new);
    	double score;

    	try {
    		// Execute the query against the REST API and receive its response
    		CloseableHttpResponse response = httpConn.executeQuery(query);
    		
    		if ((response != null) && (response.getStatusLine().getStatusCode() == 200)) {	// Response is valid
    			
    			HttpEntity entity = response.getEntity();
				if (entity != null) {
					
					JSONObject items;
			        Object val = null;
			        String id = null;
			        IDistance distMeasure = simMeasure.getDistanceMeasure();  
			        DateTimeParser dateParser = new DateTimeParser();
			        
					// Get results as a string...
					String result = EntityUtils.toString(entity);	
					// ... and then parse its JSON contents
					try {				
						items = (JSONObject) jsonParser.parse(result);
						// Obtain the array of hits (qualifying results)
						JSONArray arrItems = (JSONArray) ((JSONObject) items.get("hits")).get("hits");
						
						// ... and iterate over them in order to populate the respective priority queue
						Iterator<Object> iterator = arrItems.iterator();

						while (iterator.hasNext()) {
							JSONObject item = (JSONObject) iterator.next();
							if (item instanceof JSONObject) {
								// Only attribute values are considered
								val = ((JSONObject)item.get("_source")).get(valColumnName);
								if (val == null)
									continue;
			            	
								id = String.valueOf(item.get("_id"));
								// Depending on the operation, cast resulting attribute value into the suitable data type
				            	switch(this.operation) {
				    	        case Constants.NUMERICAL_TOPK:
				    	        	val = Double.parseDouble(String.valueOf(val));
			//	    	        	dist = distMeasure.calc(val);
				    	        	break;
				    	        case Constants.TEMPORAL_TOPK:  
				    	        	// Parse date/time values and convert them to epoch (double) values in order to compute the metric distance
				    	        	val = dateParser.parseDateTimeToEpoch(String.valueOf(val));
			//	    	        	dist = distMeasure.calc(val);
				    	        	break;
				    	        case Constants.CATEGORICAL_TOPK:   // Must convert the array of strings into a set of tokens
				    	        	// Specific for ElasticSearch: Expunge possible double quotes from the return value
				    	        	// CAUTION! Comma is the delimiter in ElasticSearch arrays
					    			val = myAssistant.tokenize(id, String.valueOf(val).replace("\"", ""), ",");  
				    				break;
				    	        case Constants.TEXTUAL_TOPK:  // Must handle non-tokenized strings using the default QGRAM value
				    	        	// Specific for ElasticSearch: Expunge possible double quotes from the return value
				    	        	val = myAssistant.tokenize(id, String.valueOf(val).replace("\"", ""), Constants.QGRAM);
				    				break;
				    	        case Constants.SPATIAL_KNN:
				    	        	// CAUTION! Geo-points in ElasticSearch are expressed as a string with the format: "lat, lon"
				    	        	String[] coords = String.valueOf(val).split(",");
				    	        	val = wktReader.read("POINT (" + coords[1] + " " + coords[0] + ")");   // Reverse coordinates for WKT representation
				    	        	break;
				            	}
				            	
//				            	System.out.println(String.valueOf(item.get("_id")) + " : "+ String.valueOf(val) + " -> " + simMeasure.scoring(distMeasure.calc(val)));
			            	
						    	// Casting the attribute value to the respective data type used by the look-up (hash) table
						    	this.datasets.get(this.hashKey).put((K)id, (V)val);

						    	// Initially keep all results sorted by ascending (original) distance
				            	score = distMeasure.calc(val);   // CAUTION! scores from ElasticSearch are ignored; recomputed according to the relevant measure
				            	resQueue.put(score, (new PartialResult(id, val, score)));
				            	numMatches++;  
				            	
				            	// The top-k distance will become the scale factor for scoring
				            	if (numMatches == topk)
				            		simMeasure.setScaleFactor(score);
							}
						}	
					} catch (Exception e) {  
						e.printStackTrace(); 
					}
				}
				response.close();   // Close the response once query result has been obtained

				// SCORING STEP
				// Once all results have been obtained, copy them to the priority queue with the adequate scaled scores (in descending order)
				for (Map.Entry<Double, PartialResult> entry : resQueue.entries()) {			
					// Result should get a score according to exponential decay function
					score = simMeasure.scoring(entry.getKey());
					entry.getValue().setScore(score);
					// Results should be inserted with descending scores in this priority queue
					partialResults.add(entry.getValue());
				}		
    		}			
    	} catch (ParseException | IOException e) {
			e.printStackTrace();		
    	} 
/*
    	for (PartialResult p: partialResults) {
    		System.out.println(p.getId() + ": " + p.getValue().toString() + " --> " + p.getScore());
    	}
*/ 
    	duration = System.nanoTime() - duration;
    	this.log.writeln("Query [" + myAssistant.decodeOperation(this.operation) + "] on " + this.valColumnName + " (in-situ) returned " + numMatches + " results in " + duration / 1000000000.0 + " sec.");
    	
    	return numMatches;                      //Report how many records have been retrieved from the database
     }
    
    
 	/**
 	 * Sends the specified similarity search request against the REST API.
 	 */
	@Override
	public void run() {
	      running.set(true);
	      
	      // Send the request and populate the queue with its results
	      compute(this.collectionSize, this.resultsQueue);
	      
	      running.set(false);
	}

 	/**
 	 * Progressively provides the next query result.
 	 * NOT applicable with this type of search, as results are issued directly to the queue.
 	 */
	@Override
	public List<V> getNextResult() {

		return null;
	}
	
}
