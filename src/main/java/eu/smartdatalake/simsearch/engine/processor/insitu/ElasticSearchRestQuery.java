package eu.smartdatalake.simsearch.engine.processor.insitu;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
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
	
    String keyColumnName = null;
	String valColumnName = null;
	ISimilarity simMeasure;
	RankedList resultsQueue;
	public int collectionSize;
	int topk;
	int operation;       		//Type of the search query
	String hashKey = null;   	// The unique hash key assigned to this search query
	
	HttpRestConnector httpConn = null;
	
	Map<String, HashMap<K,V>> datasets = null;
	
	// Parsers for values of complex data types
	DateTimeParser dateParser;
	WKTReader wktReader;
	List<String> attrCoords = Arrays.asList(new String[]{"lon", "lat"});   // Special handling for JSON with coordinates
	
	
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
		this.keyColumnName = keyColumnName;
		this.simMeasure = simMeasure;
		this.resultsQueue = resultsQueue;
		this.datasets = datasets;
		this.topk = topk;
		
		dateParser = new DateTimeParser();
		wktReader = new WKTReader();

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
			if (filter.matches("\\[[^\\[]*\\]|\\{(.*?)\\}"))
				query = "{\"bool\": {\"must\": [" + query + "], \"filter\": " + filter + "}}";
			else
				System.out.println("NOTICE: Unsupported boolean filters specified in this query will be ignored.");
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
			        IDistance distMeasure = simMeasure.getDistanceMeasure();  
			        
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
						    	// Casting the attribute value to the respective data type used by the look-up (hash) table
								ImmutablePair<String, Object> res = formatResult(item);
								if (res == null)
									continue;
								this.datasets.get(this.hashKey).put((K)res.getKey(), (V)res.getValue());

						    	// Initially keep all results sorted by ascending (original) distance
				            	score = distMeasure.calc(res.getValue());   // CAUTION! scores from ElasticSearch are ignored; recomputed according to the relevant measure
				            	resQueue.put(score, (new PartialResult(res.getKey(), res.getValue(), score)));
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
    	
    	return numMatches;	//Report how many records have been retrieved from the data source
     }
    

    /**
     * Formats the given result of an entity according to the data type of the queried attribute. 
     * @param item  A JSON object representing the value of an entity at the queried attribute.
     * @return	A (key, value) pair with the entity identifier and the attribute value formatted according to its data type.
     */
     private ImmutablePair<String, Object> formatResult(JSONObject item) {
    	 
    	JSONObject flattenedItem = null;
    	flattenedItem = flatten(((JSONObject)item.get("_source")), flattenedItem, null);
			
		// Only attribute values are considered
		Object val = flattenedItem.get(valColumnName);
		if (val == null)
			return null;
		
		// Identifier
		String id = String.valueOf(flattenedItem.get(keyColumnName));

    	try {
			// Depending on the operation, cast resulting attribute value into the suitable data type
	    	switch(this.operation) {
	        case Constants.NUMERICAL_TOPK:
	        	val = Double.parseDouble(String.valueOf(val));
	        	break;
	        case Constants.TEMPORAL_TOPK:  
	        	// Parse date/time values and convert them to epoch (double) values in order to compute the metric distance
	        	val = dateParser.parseDateTimeToEpoch(String.valueOf(val));
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
	    } catch (Exception e) {  
			e.printStackTrace(); 
		}
    	
    	return new ImmutablePair<>(id, val);	
    }
    
     
	/**
	 * Connects to a REST API to collect attribute values regarding specific object identifiers. These values are appended to the in-memory lookup maintained for this attribute.
	 * @param identifiers  The set of object identifiers (acting as the primary key in the respective database table).
	 * @return  The number of collected results.
	 */
    public int appendValues(Set<String> identifiers) {
    	
    	JSONParser jsonParser = new JSONParser();
    	int numMatches = 0;

    	try {
			String ids = String.join(",", identifiers.stream().map(id -> ("\"" + id + "\"")).collect(Collectors.toList()));
			
			// Modify the template to return all values for the given set of identifiers
			String query = queryValueRetrievalTemplate.replace("\"$id\"", ids);

    		// Execute the query against the REST API and receive its response
    		CloseableHttpResponse response = httpConn.executeQuery(query);
    		
    		if ((response != null) && (response.getStatusLine().getStatusCode() == 200)) {	// Response is valid
    			
    			HttpEntity entity = response.getEntity();
				if (entity != null) {
					
					JSONObject items;
			        
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
						    	// Casting the attribute value to the respective data type used by the look-up (hash) table
								ImmutablePair<String, Object> res = formatResult(item);
								if (res == null)
									continue;
								this.datasets.get(this.hashKey).put((K)res.getKey(), (V)res.getValue());

				            	numMatches++;
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
    	
    	return numMatches;		//Report how many records have been retrieved from the data source
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
	

	/**
	 * 
	 * @param object  The nested JSON object to be flattened.
	 * @param flattened  Container of a JSON object during flattening.
	 * @param prefix  The prefix to append in each key when flattening at the current level.
	 * @return  The resulting flattened JSON object with composite keys at a single level.
	 */
	private JSONObject flatten(JSONObject object, JSONObject flattened, String prefix) {
	    if (flattened == null) {  // Top level
	        flattened = new JSONObject();
	    }
	    for (Object key: object.keySet()) {
	    	String strKey = ((prefix != null) ? (prefix + "." + key.toString()) : key.toString());
	        try {
	            if (object.get(key) instanceof JSONObject) {   // 
	            	JSONObject json = (JSONObject)object.get(key);
	            	if (json.keySet().containsAll(attrCoords)) // JSON contains coordinates
	            		flattened.put(strKey, json.get("lon").toString() + "," + json.get("lat").toString());
	            	else		// Go one level down and continue flattening
	            		flatten(json, flattened, strKey);
	            } 
	            else if (object.get(key) instanceof JSONArray) {  // Flatten each item in JSON array
	            	JSONArray jsonArray = (JSONArray)object.get(key);
	            	for (int i = 0; i < jsonArray.size(); i++) {
	            		if (jsonArray.get(i) instanceof JSONObject) {
	            			flatten((JSONObject)jsonArray.get(i), flattened, strKey);
	            		}
	    			}
	            } 
	            else {  // Bottom level, extract values
	            	if (flattened.get(strKey) != null) // Key exists, so concatenate this value to existing ones
	            		flattened.put(strKey, flattened.get(strKey).toString() + "," + object.get(key).toString());
	            	else  	// New key
	            		flattened.put(strKey, object.get(key));
	            }
	        } catch(Exception e){
	            System.out.println(e);
	        }
	    }
	    
	    return flattened;
	}
	
}
