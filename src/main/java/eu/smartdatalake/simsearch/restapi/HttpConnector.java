package eu.smartdatalake.simsearch.restapi;

import java.io.IOException;
import java.net.URI;
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

import eu.smartdatalake.simsearch.IDataConnector;

/**
 * Class that defines all methods available by a HTTP connector.
 * Every HTTP connection to any REST API should support the same query functionality.
 */
public class HttpConnector implements IDataConnector {

	private URI uri;
	private String api_key = null;
	CloseableHttpClient httpClient;

	
	/**
	 * Auxiliary class for specifying GET http requests.
	 */
	class HttpGetWithEntity extends HttpPost {

		public final String METHOD_NAME = "POST";

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
	 * Constructor of this class.
	 * @param uri  The URI to which the REST API listens to for requests. 
	 */
	public HttpConnector(URI uri) {
		this.uri = uri;
	}
	
	/**
	 * Constructor of this class.
	 * @param uri  The URI to which the REST API listens to for requests.
	 * @param api_key
	 */
	public HttpConnector(URI uri, String api_key) {
		this.uri = uri;
		this.api_key = api_key;
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
			HttpGetWithEntity request = new HttpGetWithEntity(this.uri);
			request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
			request.addHeader(HttpHeaders.TIMEOUT, "60000");   // Specify a timeout after 60 seconds
			if (this.api_key != null)
				request.addHeader("api_key", this.api_key);    // FIXME: Specifically for requests in another SimSearch service
				//request.addHeader(HttpHeaders.AUTHORIZATION, "ApiKey XXXXXXXXXXXXXXXXXX");
			StringEntity data = new StringEntity(query);
			request.setEntity(data);
			// Execute it and get the response
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
//		 	System.out.println(query);

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
						// Obtain the array of hits (qualifying results); FIXME: specific for Elasticsearch
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
	
}
