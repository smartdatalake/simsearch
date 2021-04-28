package eu.smartdatalake.simsearch.engine.processor.insitu;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.locationtech.jts.io.WKTReader;

import eu.smartdatalake.simsearch.Assistant;
import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.engine.measure.ISimilarity;
import eu.smartdatalake.simsearch.engine.processor.ISimSearch;
import eu.smartdatalake.simsearch.engine.processor.ranking.PartialResult;
import eu.smartdatalake.simsearch.engine.processor.ranking.RankedList;
import eu.smartdatalake.simsearch.manager.ingested.temporal.DateTimeParser;
import eu.smartdatalake.simsearch.manager.insitu.HttpRestConnector;


/**
 * Implements functionality for various similarity search queries (numerical, categorical, spatial) against a SimSearch REST API.
 * CAUTION! This request always affects a SINGLE attribute; even in the case of POINT locations their lon/lat coordinates are specified as a WKT string.
 * @param <K>  Type variable representing the keys of the stored objects (i.e., primary keys).
 * @param <V>  Type variable representing the values of the stored objects (i.e., their values on a given attribute).
 */
public class SimSearchRestQuery<K extends Comparable<? super K>, V> implements ISimSearch<K, V>, Runnable {

	Logger log = null;
	Assistant myAssistant;
	public AtomicBoolean running = new AtomicBoolean(false);
	
    String query = null;			// Query to be composed for top-k search
	String valColumnName = null;
	ISimilarity simMeasure;
	RankedList resultsQueue;
	public int collectionSize;
	int operation;       		//Type of the search query
	String hashKey = null;   	// The unique hash key assigned to this search query
	
	HttpRestConnector httpConn = null;
	
	Map<String, HashMap<K,V>> datasets = null;
	
	// NO query template for value retrieval; random access to a SimSearch REST API is prohibited
	public String queryValueRetrievalTemplate = null;	
	
	/**
	 * Constructor
	 * @param httpConn  The HTTP connection that provides access to the data.
	 * @param operation  The type of the similarity search query (0: CATEGORICAL_TOPK, 1: SPATIAL_KNN, 2: NUMERICAL_TOPK).
	 * @param keyColumnName  Name of the attribute holding the entity identifiers (keys).
	 * @param valColumnName  Name of the attribute containing numerical values of these entities.
	 * @param searchValue  String specifying the query value according to the type os the search operation (i.e., keywords, a location, or a number).
	 * @param collectionSize  The count of results to fetch.
	 * @param simMeasure  The similarity measure to be used in the search.
	 * @param resultsQueue  Queue to collect query results.
	 * @param datasets  Dictionary of the attribute data available for search.
	 * @param hashKey  The unique hash key assigned to this search query.
	 * @param log  Handle to the log file for keeping messages and statistics.
	 */
	public SimSearchRestQuery(HttpRestConnector httpConn, int operation, String keyColumnName, String valColumnName, String searchValue, int collectionSize, ISimilarity simMeasure, RankedList resultsQueue, Map<String, HashMap<K,V>> datasets, String hashKey, Logger log) {
		  
		super();
		
		this.log = log;
		myAssistant = new Assistant();
		this.hashKey = hashKey;
		this.operation = operation;
		this.valColumnName = valColumnName;
		this.simMeasure = simMeasure;
		this.resultsQueue = resultsQueue;
		this.datasets = datasets;
		this.httpConn = httpConn;
		
		// Limit results returned per request up to a certain number
		// By default setting for the max number of returned results (if applied by the HTTP server)
		int maxSize = httpConn.getMaxResultCount();
		this.collectionSize = ((collectionSize > maxSize) ? maxSize : collectionSize);  
		
    	// Construct search request on a SINGLE attribute according to the specification of the REST API
		// Only one weight (1.0) specified in the query, in order to fetch results with their scores as computed by the respective distance measure
		query = "{\"k\":\"" + this.collectionSize + "\",\"queries\":[{\"operation\":\"" +  myAssistant.decodeOperation(operation) +  "\",  \"column\":" + (valColumnName.startsWith("[") ? valColumnName.replace(" ","").replace("[","[\"").replace("]","\"]").replace(",","\",\"") : "\"" + valColumnName + "\"") + ", \"value\":\"" + searchValue + "\", \"weights\": [\"1.0\"] }]}";
//		System.out.println("QUERY: " + query);		
	}
	
	
	/**
	 * Connects to a REST API and retrieves items qualifying to the submitted similarity search request.
	 * @param M  The count of results to fetch.
	 * @param partialResults  The queue that collects results obtained from the specified query.
	 * @return  The number of collected results.
	 */
    public int compute(int M, RankedList partialResults) {
    	 	 
    	JSONParser jsonParser = new JSONParser();
    	int numMatches = 0;
    	long duration = System.nanoTime();
    	double score;

    	try {
    		// Execute the query against the REST API and receive its response
    		CloseableHttpResponse response = httpConn.executeQuery(query);
    		
    		if (response != null) {								// Response is valid
    			
    			HttpEntity entity = response.getEntity();
				if (entity != null) {
					
					JSONArray attributes;
			        Object val = null;
			        String id = null;
   
					// Get results as a string...
					String result = EntityUtils.toString(entity);	

					// ... and then parse its JSON contents
					try {
						JSONArray itemsPerWeight = (JSONArray) jsonParser.parse(result);   	// Typically, the SimSearch service returns one list of results per weight
						JSONObject items = (JSONObject) itemsPerWeight.get(0);				// This request specifies a single weight (1.0), so at most one such list should be returned
						if (!items.isEmpty()) {
	
							// Instantiate a date/time parser in case of temporal attributes
							DateTimeParser dateParser = new DateTimeParser();		
							// ... and a WKT parser for spatial (location) attributes
							WKTReader wktReader = new WKTReader();
							
							// Obtain the array of ranked results qualifying to the query
							JSONArray arrItems = (JSONArray) items.get("rankedResults");

							// ... and iterate over them in order to populate the respective priority queue
							Iterator<Object> iterator = arrItems.iterator();
							while (iterator.hasNext()) {
								JSONObject item = (JSONObject) iterator.next();
								if (item instanceof JSONObject) {
									
									val = null;
									score = 0.0;
									id = String.valueOf(item.get("id"));
									attributes = (JSONArray)(item.get("attributes"));
									for (int i = 0; i < attributes.size(); i++) {
										if (((JSONObject)attributes.get(i)).get("name").equals(this.valColumnName)) {
											// Get the value at the requested attribute
											val = ((JSONObject)attributes.get(i)).get("value");
											// Also get the score as computed by the SimSearch service
											score = (double)((JSONObject)attributes.get(i)).get("score");
										}
									}
									if (val == null)
										continue;
			            	
									// Depending on the operation, cast resulting attribute value into the suitable data type
					            	switch(this.operation) {
					    	        case Constants.NUMERICAL_TOPK:
					    	        	val = Double.parseDouble(String.valueOf(val));
					    	        	break;
					    	        case Constants.TEMPORAL_TOPK:  
					    	        	// Parse date/time values and convert them to epoch (double) values in order to compute the metric distance
					    	        	val = dateParser.parseDateTimeToEpoch(String.valueOf(val));
					    	        	break;
					    	        case Constants.CATEGORICAL_TOPK:
					    	        	// Expunge possible double quotes from the return value, as well as the brackets
						    			String keywords = String.valueOf(val).replace("\"", "");
						    			keywords = keywords.substring(1, keywords.length()-1);
						    			val = myAssistant.tokenize(id, keywords, ",");   // Assuming that comma is the delimiter in string arrays
					    				break;
					    	        case Constants.TEXTUAL_TOPK:  // Must handle non-tokenized strings using the default QGRAM value
					    	        	val = myAssistant.tokenize(id, String.valueOf(val).replace("\"", ""), Constants.QGRAM);
					    				break;
					    	        case Constants.SPATIAL_KNN:
					    	        	// SimSearch returns geometries as WKT, so they should be converted into their binary representation
					    	        	val = wktReader.read(String.valueOf(val));
					    	        	break;
					            	}
					            	
							    	// Casting the attribute value to the respective data type used by the look-up (hash) table
							    	this.datasets.get(this.hashKey).put((K)id, (V)val);
	
							    	// Results sorted by descending score as obtained from the SimSearch service 
					            	partialResults.add((new PartialResult(id, val, score)));
					            	numMatches++;  
								}
							}
						}
					} catch (Exception e) {  
						e.printStackTrace(); 
					}
				}
				response.close();   // Close the response once query result has been obtained
	
    		}			
    	} catch (ParseException | IOException e) {
			e.printStackTrace();		
    	} 

    	duration = System.nanoTime() - duration;
    	this.log.writeln("Query [" + myAssistant.decodeOperation(this.operation) + "] on " + this.valColumnName + " (SimSearch service) returned " + numMatches + " results in " + duration / 1000000000.0 + " sec.");
    	
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
