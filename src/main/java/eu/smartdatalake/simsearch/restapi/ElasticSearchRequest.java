package eu.smartdatalake.simsearch.restapi;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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
import eu.smartdatalake.simsearch.ISimSearch;
import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.PartialResult;
import eu.smartdatalake.simsearch.measure.IDistance;
import eu.smartdatalake.simsearch.measure.ISimilarity;


/**
 * Implements functionality for various similarity search queries (numerical, categorical, spatial) against an Elasticsearch REST API.
 * @param <K>  Type variable representing the keys of the stored objects (i.e., primary keys).
 * @param <V>  Type variable representing the values of the stored objects (i.e., their values on a given attribute).
 * 
 * // FIXME: There seems to be a default limit of 10000 results per query. Possible remedies:
 * https://discuss.elastic.co/t/pulling-more-than-10000-records-from-elasticsearch-query/181000
 * https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-body.html#request-body-search-scroll
 */
public class ElasticSearchRequest<K extends Comparable<? super K>, V> implements ISimSearch<K, V>, Runnable {

	Logger log = null;
	Assistant myAssistant;
	public AtomicBoolean running = new AtomicBoolean(false);
	
	URI uri = null;
    String query = null;			// Query to be composed for top-k search
	
	String valColumnName = null;
	ISimilarity simMeasure;
	ConcurrentLinkedQueue<PartialResult> resultsQueue;
	public int collectionSize;
	int operation;       		//Type of the search query
	String hashKey = null;   	// The unique hash key assigned to this search query
	
	HttpConnector httpConn = null;
	
	Map<String, HashMap<K,V>> datasets = null;
	
	// Compose the query template for value retrieval 
	public String queryValueRetrievalTemplate = null;	

	/**
	 * Constructor
	 * @param uri
	 * @param operation
	 * @param keyColumnName
	 * @param valColumnName
	 * @param searchValue
	 * @param collectionSize
	 * @param simMeasure
	 * @param resultsQueue
	 * @param log
	 */
	public ElasticSearchRequest(HttpConnector httpConn, int operation, String keyColumnName, String valColumnName, String searchValue, int collectionSize, ISimilarity simMeasure, ConcurrentLinkedQueue<PartialResult> resultsQueue, Map<String, HashMap<K,V>> datasets, String hashKey, Logger log) {
		  
		super();

		this.log = log;
		myAssistant = new Assistant();
		this.hashKey = hashKey;
		this.operation = operation;
		this.valColumnName = valColumnName;
		this.simMeasure = simMeasure;
		this.resultsQueue = resultsQueue;
		this.datasets = datasets;
		// TODO: By default setting for the max number of returned results
		this.collectionSize = 10000; // collectionSize;  
		this.httpConn = httpConn;
		
    	// Construct search request according to the type of the operation
    	// FIXME: REST APIs may have different specifications for the various types of queries; currently using the ElasticSearch dialect
		// FIXME: Elasticsearch is sensitive to scale and decay parameters; reduced decay values miss several more relevant results 
		if (operation == Constants.NUMERICAL_TOPK) {
			query = "{\"_source\": [\"" + keyColumnName + "\", \"" + valColumnName + "\"]," + "\"query\": {\"function_score\": {\"query\": {\"exists\": { \"field\": \"" + valColumnName + "\" }},"
				+ "\"exp\": {\"" + valColumnName + "\": {\"origin\": \"" + searchValue 
				+ "\",\"scale\": \"1\",\"decay\" : 0.99999}}}},\"size\": " + this.collectionSize + "}";
			}
		else if (operation == Constants.CATEGORICAL_TOPK) {
			query = "{\"_source\": [\"" + keyColumnName + "\", \"" + valColumnName + "\"]," + "\"query\": { \"match\": { \"" + valColumnName + "\": \"" + searchValue + "\" } }," + "\"size\": " + this.collectionSize + "}";
			}
		else if (operation == Constants.SPATIAL_KNN) {
			query = "{\"_source\": [\"" + keyColumnName + "\", \"" + valColumnName + "\"]," + "\"query\": {\"function_score\": {\"exp\": {\"" + valColumnName + "\": {\"origin\": \"" + searchValue 
				+ "\", \"scale\": \"100m\",\"decay\" : 0.99999}}}}," + "\"size\": " + this.collectionSize + "}";
			}	
		
		// Template of the query that retrieves the value for a particular object ($id is a placeholder for its identifier)
		queryValueRetrievalTemplate = "{\"_source\": [\"" + keyColumnName + "\", \"" + valColumnName + "\"]," + "\"query\": {\"ids\": {\"values\": [\"$id\"]}}}";
	}

	
	/**
	 * Connects to a REST API and retrieves items qualifying to the submitted similarity search request.
	 */
    public int compute(int k, ConcurrentLinkedQueue<PartialResult> partialResults) {
    	
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
    		
    		if (response != null) {								// Response is valid
    			
    			HttpEntity entity = response.getEntity();
				if (entity != null) {
					
					JSONObject items;
			        Object val = null;
			        IDistance distMeasure = simMeasure.getDistanceMeasure();
			        
					// Get results as a string...
					String result = EntityUtils.toString(entity);	
					// ... and then parse its JSON contents
					try {				
						items = (JSONObject) jsonParser.parse(result);
						// Obtain the array of hits (qualifying results)
						JSONArray arrItems = (JSONArray) ((JSONObject) items.get("hits")).get("hits");
	//					System.out.println("RESULTS: " + arrItems.size());
						
						// ... and iterate over them in order to populate the respective priority queue
						Iterator<Object> iterator = arrItems.iterator();
						while (iterator.hasNext()) {
							JSONObject item = (JSONObject) iterator.next();
							if (item instanceof JSONObject) {
		//	            		System.out.println(String.valueOf(item.get("_id")) + " : "+ String.valueOf(((JSONObject)item.get("_source")).get(valColumnName)) + " -> " + String.valueOf(item.get("_score")));
								val = ((JSONObject)item.get("_source")).get(valColumnName);
								if (val == null)
									continue;
//								else 
//									System.out.println(String.valueOf(item.get("_id")) + " : "+ String.valueOf(val));
			            	
								// Depending on the operation, cast resulting attribute value into the suitable data type
				            	switch(this.operation) {
				    	        case Constants.NUMERICAL_TOPK:
				    	        	val = Double.parseDouble(String.valueOf(val));
			//	    	        	dist = distMeasure.calc(val);
				    	        	break;
				    	        case Constants.CATEGORICAL_TOPK:
				    	        	// FIXME: Specific for ElasticSearch: Expunge possible double quotes from the return value
					    			String keywords = String.valueOf(val).replace("\"", "");
			//		    			keywords = keywords.substring(1, keywords.length()-1);
					    			val = myAssistant.tokenize(String.valueOf(item.get("_id")), keywords, ",");   // FIXME: comma is the delimiter in ElasticSearch arrays
				    				break;
				    	        case Constants.SPATIAL_KNN:
				    	        	// FIXME: Geo-points in ElasticSearch are expressed as a string with the format: "lat, lon"
				    	        	String[] coords = String.valueOf(val).split(",");
				    	        	val = wktReader.read("POINT (" + coords[1] + " " + coords[0] + ")");   // Reverse coordinates for WKT representation
				    	        	break;
				            	}
				            	
//				            	System.out.println(String.valueOf(item.get("_id")) + " : "+ String.valueOf(val) + " -> " + simMeasure.scoring(distMeasure.calc(val)));
			            	
						    	// Casting the attribute value to the respective data type used by the look-up (hash) table
						    	this.datasets.get(this.hashKey).put((K)String.valueOf(item.get("_id")), (V)val);

						    	// Initially keep all results sorted by ascending (original) distance
				            	score = distMeasure.calc(val);   
				            	resQueue.put(score, (new PartialResult(String.valueOf(item.get("_id")), val, score)));
				            	numMatches++;  
							}
						}	
					} catch (Exception e) {  
						e.printStackTrace(); 
					}
				}
				response.close();   // Close the response once query result has been obtained

				// Once all results have been obtained, copy them to the priority queue with the adequate scaled scores (in descending order)
				for (Map.Entry<Double, PartialResult> entry : resQueue.entries()) {			
					// SCORING STEP: Result should get a score according to exponential decay function
					score = simMeasure.scoring(entry.getKey());
					entry.getValue().setScore(score);
					// TODO: Verify that results are entered with descending scores in this priority queue
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
    	this.log.writeln("Query [" + myAssistant.descOperation(this.operation) + "] " + this.hashKey + " (in-situ) returned " + numMatches + " results in " + duration / 1000000000.0 + " sec.");
    	
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
