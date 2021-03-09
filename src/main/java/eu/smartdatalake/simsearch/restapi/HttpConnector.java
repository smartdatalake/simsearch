package eu.smartdatalake.simsearch.restapi;

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

import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.manager.IDataConnector;

/**
 * Class that defines all methods available by a HTTP connector.
 * Every HTTP connection to any REST API should support the same query functionality.
 */
public class HttpConnector implements IDataConnector {

	private URI uri;
	private String username = null;
	private String password = null;
	private String api_key = null;
	CloseableHttpClient httpClient;
	
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
	public HttpConnector(URI uri) {
		this.uri = uri;
	}
	
	
	/**
	 * Constructor #2 of this class.
	 * @param uri  The URI to which the REST API listens to for requests.
	 * @param api_key  The API key necessary to establish connection to this REST API.
	 */
	public HttpConnector(URI uri, String api_key) {
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
	public HttpConnector(URI uri, String username, String password) {
		this.uri = uri;
		this.username = username;
		this.password = password;
		
		// Keep track of the max number of results per request (if applicable)
		this.maxResultCount = findMaxResultCount();
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
	 		JSONParser jsonParser = new JSONParser();
	
	 		if (response != null) {	
				HttpEntity entity = response.getEntity();

				if (entity != null) {
					
					// Get results as a string...
					String result = EntityUtils.toString(entity);	
					// ... and then parse its JSON contents
					try {				
						JSONObject items = (JSONObject) jsonParser.parse(result);
						// Obtain the array of hits (qualifying results); FIXME: custom handling for Elasticsearch
						JSONArray arrItems = (JSONArray) ((JSONObject) items.get("hits")).get("hits");
						
						// ... and iterate over them in order to populate the respective priority queue
						Iterator<Object> iterator = arrItems.iterator();
						while (iterator.hasNext()) {
							JSONObject item = (JSONObject) iterator.next();
							if(item instanceof JSONObject) {
								val = ((JSONObject)item.get("_source")).values().iterator().next();
							}
						}
					} catch (Exception e) {  e.printStackTrace(); }

				}
				response.close();   // Close the response once query result has been obtained
	 		}
	 
	 	 } catch (Exception e) {
	 		 e.printStackTrace();
	 	 }
	 	 
	 	 return val;
	} 
	  
	  
	/**
	 * Open a connection to the REST API using default settings.
	 */
	public void openConnection() { 
		
		this.httpClient = HttpClients.createDefault();
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
	 * Sets header information at an HTTP request
	 * @param request  The HTTP request.
	 */
	private void setHeader(HttpPost request) {
		
		try {	
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
		}
		catch (Exception e) {
	   		 e.printStackTrace();
	   	}
	}
	
	
	/**
	 * Finds out the maximum number of results returned by an HTTP request.
	 * @return  An integer value respresenting the maximum number of results per HTTP request.
	 */
	private int findMaxResultCount() {
		
		int maxSize = Constants.INFLATION_FACTOR;
		
	 	try { 		
	 		String origURI = this.uri.toString();
	 		
	 		// SimSearch REST API does not specify this value
	 		if (origURI.contains("simsearch"))
	 			return maxSize;
	 		
	 		// FIXME: This URI specifically targeting ElasticSearch indices
	 		String settingsURI = origURI.substring(0, origURI.indexOf("/_")) + "/_settings";
	 		System.out.println("URI:" + new URI(settingsURI));
	 		
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
		 		if (response != null) {	
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
							// FIXME: Value extracted according to the JSON response by ElasticSearch
							maxSize = Integer.parseInt((String) getJSONValue(getJSONValue(getJSONValue(value, "settings"), "index"),"max_result_window"));
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
