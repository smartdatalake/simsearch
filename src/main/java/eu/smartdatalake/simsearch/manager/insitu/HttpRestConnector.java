package eu.smartdatalake.simsearch.manager.insitu;

import java.io.IOException;
import java.net.URI;
import java.util.Base64;
import java.util.Iterator;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import eu.smartdatalake.simsearch.Assistant;
import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.manager.IDataConnector;

/**
 * Class that defines all methods available by a HTTP connector.
 * Every HTTP connection to any REST API should support the same query functionality.
 */
public class HttpRestConnector implements IDataConnector {

	public URI uri;
	private String username = null;
	private String password = null;
	private String api_key = null;
	CloseableHttpClient httpClient;
	Assistant myAssistant;
	private boolean isSimSearchInstance;
	private int maxResultCount;   // Max number of returned results (typically used in ElasticSearch)

	
	/**
	 * Auxiliary class for specifying GET http requests.
	 */
	class HttpGetWithEntity extends HttpPost {

		public final String METHOD_NAME = "GET";

		public HttpGetWithEntity(URI url) {
			super(url);
		}

		public HttpGetWithEntity(String url) {
			super(url);
		}

		@Override
		public String getMethod() {
			return METHOD_NAME;
		}
	}
	
	/**
	 * Auxiliary class for specifying POST http requests.
	 */
	class HttpPostWithEntity extends HttpPost {

		public final String METHOD_NAME = "POST";

		public HttpPostWithEntity(URI url) {
			super(url);
		}

		public HttpPostWithEntity(String url) {
			super(url);
		}

		@Override
		public String getMethod() {
			return METHOD_NAME;
		}
	}
	
	
	/**
	 * Constructor #1 of this class.
	 * @param uri  The URI to which the REST API listens to for requests. 
	 */
	public HttpRestConnector(URI uri) {
		this.uri = uri;
		
		// Keep track of the max number of results per request (if applicable)
		this.maxResultCount = findMaxResultCount();
	}
	
	
	/**
	 * Constructor #2 of this class.
	 * @param uri  The URI to which the REST API listens to for requests.
	 * @param api_key  The API key necessary to establish connection to this REST API.
	 */
	public HttpRestConnector(URI uri, String api_key) {
		this.uri = uri;
		this.api_key = api_key;
		
		// Keep track of the max number of results per request (if applicable)
		this.maxResultCount = findMaxResultCount();
	}
	
	
	/**
	 * Constructor #3 of this class.
	 * @param uri  The URI to which the REST API listens to for requests.
	 * @param username  The username required for authorized access to the REST API.
	 * @param password  The password required for authorized access to the REST API.
	 */
	public HttpRestConnector(URI uri, String username, String password) {
		this.uri = uri;
		this.username = username;
		this.password = password;
		
		// Keep track of the max number of results per request (if applicable)
		this.maxResultCount = findMaxResultCount();
	}
	
	
	/** 
	 * Specifies whether this HTTP connection refers to another SimSearch instance or a REST API.
	 * @param flag  True, if connecting to SimSearch REST API; otherwise, False.
	 */
	public void setSimSearchInstance(boolean flag) {
		isSimSearchInstance = flag;
	}
	
	
	/**
	 * Examines whether this HTTP connection refers to another SimSearch instance or a REST API.
	 * @return  True, if connecting to SimSearch REST API; otherwise, False.
	 */
	public boolean isSimSearchInstance() {
		return isSimSearchInstance;
	}
	
	
	/**
	 * Executes the specified queried against the REST API.
	 * @param query  The query submitted for execution (in JSON).
	 * @return  The response of the REST API to the submitted query.
	 */
	public CloseableHttpResponse executeQuery(String query) {
		
		CloseableHttpResponse response;                // Response will be closed by the process that consumes its results
		try {	
			// Formulate the request to be sent
			HttpPostWithEntity request = new HttpPostWithEntity(this.uri);
			setHeader(request);
/*			
			request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
			request.addHeader(HttpHeaders.TIMEOUT, "60000");   // Specify a timeout after 60 seconds
			
			// Encode username and password credentials for authorized access
			if ((this.username != null) && (this.password != null)) {
				String encoding = Base64.getEncoder().encodeToString((this.username.concat(":").concat(this.password)).getBytes("UTF-8"));
				request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoding);
			}
	        
			// Custom use of API key as required for requests in another SimSearch service
			if (this.api_key != null) {
				request.addHeader("api_key", this.api_key);    
				//request.addHeader(HttpHeaders.AUTHORIZATION, "ApiKey XXXXXXXXXXXXXXXXXX"); // NOT USED: API key is included in the authorization header
			}
*/			
			// Specify the query for this requests and execute it and get the response
			StringEntity data = new StringEntity(query);
			request.setEntity(data);
			response = httpClient.execute(request);
			return response;
		} 
		catch (ClientProtocolException e) {
	   		 e.printStackTrace();
	   	}
		catch (IOException e) { 
			e.printStackTrace();
		} 
	 
	   	return null;
	}
	
	
	@Override
	public Object findSingletonValue(String query) {
		  	
	 	 Object val = null;
	 	 try {

	 		CloseableHttpResponse response = executeQuery(query);
	
	 		if ((response != null) && (response.getStatusLine().getStatusCode() == 200)) {	
				HttpEntity entity = response.getEntity();

				if (entity != null) {
					
					// Get results as a string...
					String result = EntityUtils.toString(entity);	
					// ... and then parse its JSON contents
					if (isSimSearchInstance)
						val = parseSimSearchResult(result);
					else
						val = parseElasticSearchResult(result);
				}
				response.close();   // Close the response once query result has been obtained
	 		}
	 
	 	 } catch (Exception e) {
	 		 e.printStackTrace();
	 	 }
	 	 
	 	 return val;
	} 

	
	/**
	 * Parses the JSON response obtained from a query on a single attribute in ElasticSearch.
	 * @param jsonResult  A string representation of the full response.
	 * @return  A value on the attribute specified in the query.
	 */
	private Object parseElasticSearchResult(String jsonResult) {
		
		JSONParser jsonParser = new JSONParser();
		Object val = null;
		try {				
			JSONObject items = (JSONObject) jsonParser.parse(jsonResult);
			// Obtain the array of hits (qualifying results); custom handling for ElasticSearch
			JSONArray arrItems = (JSONArray) ((JSONObject) items.get("hits")).get("hits");
			// ... and iterate over them in order to populate the respective priority queue
			Iterator<Object> iterator = arrItems.iterator();
			while (iterator.hasNext()) {
				JSONObject item = (JSONObject) iterator.next();
				if (item instanceof JSONObject) {
					val = ((JSONObject)item.get("_source")).values().iterator().next();
				}
			}
		} catch (Exception e) {  e.printStackTrace(); }
		
		return val;
	}
	
	
	/**
	 * Parses the JSON response obtained from a Catalog request on a single attribute against another SimSearch instance.
	 * @param jsonResult  A string representation of the full response.
	 * @return  A value on the attribute specified in the Catalog request.
	 */
	private Object parseSimSearchResult(String jsonResult) {
		
		JSONParser jsonParser = new JSONParser();
		Object val = null;
		try {	
			// Obtain the array of attributes
			JSONArray arrItems = (JSONArray) jsonParser.parse(jsonResult);		
			// ... and iterate over them in order to identify the given one
			Iterator<Object> iterator = arrItems.iterator();
			while (iterator.hasNext()) {
				JSONObject item = (JSONObject) iterator.next();
				if (item instanceof JSONObject) {
					val = item.get("sampleValue");
				}
			}
		} catch (Exception e) {  e.printStackTrace(); }
		
		return val;
	}
	
	
	/**
	 * Open a connection to the REST API using default settings.
	 */
	public void openConnection() { 
		
		this.httpClient = HttpClients.createDefault();
	}
	
	/**
	 * Open a connection to the specified uri.
	 * @param uri  The uri to be used for connecting to the REST API.
	 */
	public void openConnection(URI uri) { 
		this.uri = uri;
		openConnection();
	}
	
	
	/**
	 * Closes the connection to the REST API.
	 */
	public void closeConnection() {

		try { 
			httpClient.close(); 
		}  
		catch (IOException e) { 
			e.printStackTrace(); 
		} 
	}
	
	
	/**
	 * Closes the connection to the REST API and resets the uri to the given value.
	 * @param uri  The uri to be used for FUTURE connections to the REST API.
	 */
	public void closeConnection(URI uri) { 
		closeConnection(); 
		this.uri = uri;
	}
	
	
	/**
	 * Sets header information at an HTTP request
	 * @param request  The HTTP request.
	 */
	private void setHeader(HttpPost request) {
		
		try {	
			request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
			request.addHeader(HttpHeaders.TIMEOUT, "60000");   // Specify a default timeout after 60 seconds
			
			// Encode username and password credentials for authorized access
			if ((this.username != null) && (this.password != null)) {
				String encoding = Base64.getEncoder().encodeToString((this.username.concat(":").concat(this.password)).getBytes("UTF-8"));
				request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoding);
			}
	        
			// Custom use of API key as required for requests in another SimSearch service
			if (this.api_key != null) {
				request.addHeader("api_key", this.api_key);    
				//request.addHeader(HttpHeaders.AUTHORIZATION, "ApiKey XXXXXXXXXXXXXXXXXX"); // NOT USED: API key is included in the authorization header
			}
		}
		catch (Exception e) {
	   		 e.printStackTrace();
	   	}
	}
	
	/**
	 * Checks whether the given attribute name is available for queries.
	 * FIXME: This is currently implemented for .
	 * @param fieldName  The attribute name.
	 * @return  True, if the attribute exists and contains at least one NOT NULL value; otherwise, False.
	 */
	public boolean fieldExists(String fieldName) {
		
		Object val = null;
		if (isSimSearchInstance) {
			// SimSearch only: examine response from a custom catalog request
			val = findSingletonValue("{\"column\": \"" + fieldName + "\"}");
		}
		else {	
			// ElasticSearch only: check if this field contains at least one value 
			val = findSingletonValue("{\"_source\": [\"" + fieldName + "\"], \"query\": {\"exists\": {\"field\": \"" +  fieldName + "\"}}}");
		}
		
		return (val != null);
	}
	
	
	/**
	 * Fetches a sample value from the given attribute.
	 * FIXME: This is currently implemented for ElasticSearch only.
	 * @param fieldName  The attribute name.
	 * @return  A sample value; its data type depends on the attribute.
	 */
	public Object getSampleValue(String fieldName) {
		
		if (isSimSearchInstance) {
			// SimSearch only: examine response from a custom catalog request
			return findSingletonValue("{\"column\": \"" + fieldName + "\"}");
		}
		else {
			// ElasticSearch only: fetch a single value from this field
			return findSingletonValue("{\"_source\": [\"" + fieldName + "\"], \"query\": {\"constant_score\" : {\"filter\" :{\"exists\": {\"field\": \"" +  fieldName + "\"}}}}, \"size\":1}");
		}
	}
	
	/**
	 * Finds out the maximum number of results returned by an HTTP request.
	 * CAUTION: This method currently handles only ElasticSearch indices as well as data sources from another SimSearch instance.
	 * @return  An integer value representing the maximum number of results per HTTP request.
	 */
	private int findMaxResultCount() {
		
		int maxSize = 0;
		myAssistant = new Assistant();
		
	 	try { 		
	 		String origURI = this.uri.toString();
	 		
	 		String settingsURI = null;
	 		if (origURI.trim().endsWith("/simsearch/api/search"))  // This URI refers to another SimSearch instance
	 			settingsURI = origURI.trim().substring(0, origURI.indexOf("/simsearch/api/search")) + "/simsearch/api/_settings";
	 		else // This URI specifically targeting ElasticSearch indices
	 			settingsURI = origURI.substring(0, origURI.indexOf("/_")) + "/_settings";
//	 		System.out.println("URI:" + new URI(settingsURI));
	 		
	 		// Create a new HTTP client to get the settings
	 		this.httpClient = HttpClients.createDefault();

	 		CloseableHttpResponse response;                // Response will be closed by the process that consumes its results
			try {	
				// Formulate the request to be sent
				HttpGetWithEntity request = new HttpGetWithEntity(new URI(settingsURI));
				setHeader(request);

				// No query required for this request regarding the settings
				StringEntity data = new StringEntity("");
				request.setEntity(data);

				// Execute the request and get the response
				response = httpClient.execute(request);
		 		if ((response != null) && (response.getStatusLine().getStatusCode() == 200)) {	
					HttpEntity entity = response.getEntity();
					
					if (entity != null) {
						
						// Get response as a string...
						String result = EntityUtils.toString(entity);	

						// ... and then parse its JSON contents
						try {
							JSONParser jsonParser = new JSONParser();
							JSONObject items = (JSONObject) jsonParser.parse(result);
							String key = (String) items.keySet().iterator().next(); 
							Object value = items.get(key);
							// Establish whether this is a SimSearch instance
							this.setSimSearchInstance(((JSONObject) getJSONValue(getJSONValue(value, "settings"), "index")).containsKey("isSimSearchInstance"));
							// Value extracted according to the JSON response by ElasticSearch or SimSearch
							String strSize = (String) getJSONValue(getJSONValue(getJSONValue(value, "settings"), "index"),"max_result_window");
							int size = 10000; // Default value;
							if (myAssistant.isNumeric(strSize))
								size = Integer.parseInt(strSize);
							if (this.isSimSearchInstance())   // For SimSearch
								maxSize = size;
							else if (size > maxSize)  // Precaution just in case ES returns a zero value
								maxSize = size;
						} catch (Exception e) {  
							e.printStackTrace(); 
						}
					}
		 		}
			} 
			catch (ClientProtocolException e) {
		   		 e.printStackTrace();
		   	}
			catch (IOException e) { 
				e.printStackTrace();
			} 
			
			// Close the connection
			httpClient.close();
	 	} catch (Exception e) {
	 		e.printStackTrace();
	 	}

		return maxSize;
	}

	
	// Auxiliary function to extract from a JSON the value at a specific key
	private Object getJSONValue(Object object, String key) {
		return ((JSONObject) object).get(key);
	}


	/**
	 * Returns the maximum number of results per HTTP request (if supported by the HTTP service).
	 * @return  An integer value.
	 */
	public int getMaxResultCount() {
		return this.maxResultCount;
	}

	/**
	 * Sets the maximum number of results per HTTP request (if supported by the HTTP service).
	 * @param maxResultCount  An integer value to limit the number of returned results.
	 */
	public void setMaxResultCount(int maxResultCount) {
		this.maxResultCount = maxResultCount;
	}
	
}
