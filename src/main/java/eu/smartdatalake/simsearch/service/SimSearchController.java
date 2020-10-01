package eu.smartdatalake.simsearch.service;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.CrossOrigin;

import io.swagger.annotations.ApiParam;

import eu.smartdatalake.simsearch.Coordinator;
import eu.smartdatalake.simsearch.Response;
import eu.smartdatalake.simsearch.SearchResponse;
import eu.smartdatalake.simsearch.AttributeInfo;
import eu.smartdatalake.simsearch.request.MountRequest;
import eu.smartdatalake.simsearch.request.CatalogRequest;
import eu.smartdatalake.simsearch.request.RemoveRequest;
import eu.smartdatalake.simsearch.request.SearchRequest;

import java.lang.Exception;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

/**
 * Controller that handles all HTTP requests to the RESTful web service.
 */
@RestController
@RequestMapping("/simsearch/api")
public class SimSearchController {

	int API_key_length = 128;   		// Standard length (in bits) for all generated API keys
	
	Map<String, Coordinator> dictCoordinators;    // Dictionary of all coordinator instances (each with its own API key) deployed at the SimSearch back-end.
	
	/**
	 * Checks whether the given API key is valid, i.e., listed among those generated for the various data sources.
	 * @param api_key  The API key to check.
	 * @return  A Boolean value: True, if the key is valid; otherwise, False.
	 */
	private boolean isValidApiKey(String api_key) {

		return dictCoordinators.containsKey(api_key) ? true : false;
	}

	/**
	 * Generates a new API key that is associated with the newly specified data source(s).
	 * @return  A string with the new API key.
	 */
    public String generateAPIkey() {

        SecureRandom random = new SecureRandom();
        byte bytes[] = new byte[API_key_length/8];
        random.nextBytes(bytes);
        return DatatypeConverter.printHexBinary(bytes).toLowerCase();
    }
    
	/**
	 * Constructor. It also instantiates the lists of API keys.
	 */
	@Autowired
	public SimSearchController() {

		// Instantiate a dictionary of all instantiated coordinators (one per generated API key)
		dictCoordinators = new HashMap<String, Coordinator>();
	}

	/**
	 * Mounts the specified data sources and associates them to a new API key, including building in-memory indices or instantiating connections to remote sources. 
	 * @param params  Parameters specified in JSON (instantiating a MountRequest object) that specify the data sources that will be enabled and their queryable attributes. 
	 * @return  A notification whether the data sources were successfully mounted or not. If successful, this request also returns an API key to be used in all subsequent requests involving these data sources.
	 */
	@CrossOrigin
	@RequestMapping(value = "/index", method = { RequestMethod.POST }, consumes = "application/json", produces = "application/json")
	public ResponseEntity<Response> index(@ApiParam("Request to method index") @RequestBody MountRequest params) {

		Response mountResponse;
		
		// Create a new instance of the coordinator at the SimSearch back-end
		Coordinator myCoordinator = new Coordinator();
		// A new API key where the specified data source(s) will be assigned to
		String apiKey;

		System.out.println("Preparing indices. This process may take a few minutes. Please wait...");

		// MOUNTING DATA SOURCE(S)
		try {	
			// Invoke mounting step
			mountResponse = myCoordinator.mount(params);
			// Generate a new API key ...
			apiKey = generateAPIkey();
			// ... that must be used in all subsequent requests against these data sources
			dictCoordinators.put(apiKey, myCoordinator);
			mountResponse.appendNotification("Mounting of data sources completed successfully. ***IMPORTANT*** In all subsequent requests regarding this data, you must specify this API key: " + apiKey);
			myCoordinator.log("Newly created data sources have been associated with this API key: " + apiKey);
			System.out.println("Mounting of data sources completed!");			
		}
		catch (Exception e) {
			e.printStackTrace();
			mountResponse = new Response();
			mountResponse.setNotification("Mounting stage terminated abnormally. Make sure that the submitted JSON configuration provides suitable specifications.");
			System.out.println("Mounting stage terminated abnormally.");
			return new ResponseEntity<>(mountResponse, HttpStatus.BAD_REQUEST);
		}

		return new ResponseEntity<>(mountResponse, HttpStatus.OK);
	}
	

	/**
	 * Appends the specified data sources to those already mounted for this API key, including building in-memory indices or instantiating connections to remote sources. 
	 * @param apiKey  The client API key associated with the corresponding data sources.
	 * @param params  Parameters specified in JSON (instantiating a MountRequest object) that specify the data sources that will be enabled and their queryable attributes. 
	 * @return  A notification whether the data sources were successfully mounted or not.
	 */
	@CrossOrigin
	@RequestMapping(value = "/append", method = { RequestMethod.POST }, consumes = "application/json", produces = "application/json")
	public ResponseEntity<Response> index(@ApiParam("The client API key allowing access to the data") @RequestHeader("api_key") String apiKey, @ApiParam("Request to method index") @RequestBody MountRequest params) {

		Response appendResponse;
		
		if (!isValidApiKey(apiKey)) {
			appendResponse = new Response();
			appendResponse.setNotification("Operation not allowed for this user. Please check your API key.");
			return new ResponseEntity<>(appendResponse, HttpStatus.FORBIDDEN);
		}
		
		// Identify the coordinator that handles existing data sources for the specified API key
		Coordinator myCoordinator = dictCoordinators.get(apiKey);

		System.out.println("Preparing indices. This process may take a few minutes. Please wait...");

		// MOUNTING DATA SOURCE(S)
		try {	
			// Invoke mounting step
			appendResponse = myCoordinator.mount(params);
			appendResponse.appendNotification("Appended data sources are now available for queries and are associated with this API key: " + apiKey);
			myCoordinator.log("Appended data sources are now available for queries and are associated with this API key: " + apiKey);
			System.out.println("Mounting of data sources completed!");			
		}
		catch (Exception e) {
			e.printStackTrace();
			appendResponse = new Response();
			appendResponse.setNotification("Mounting stage terminated abnormally. Make sure that the submitted JSON configuration provides suitable specifications.");
			System.out.println("Mounting stage terminated abnormally.");
			return new ResponseEntity<>(appendResponse, HttpStatus.BAD_REQUEST);
		}

		return new ResponseEntity<>(appendResponse, HttpStatus.OK);
	}


	/**
	 * Lists the queryable attributes that may be included in similarity search requests.
	 * @param apiKey  The client API key associated with the corresponding data sources.
	 * @param params Parameters specified in JSON (instantiating a CatalogRequest object); if empty {}, then all sources will be listed. 
	 * @return  A JSON listing the catalog of queryable attributes and the similarity operation supported by each one.
	 */
	@CrossOrigin
	@RequestMapping(value = "/catalog", method = { RequestMethod.POST }, consumes = "application/json", produces = "application/json")
	public ResponseEntity<AttributeInfo[]> catalog(@ApiParam("The client API key") @RequestHeader("api_key") String apiKey, @ApiParam("Request to method index") @RequestBody CatalogRequest params) {

		if (!isValidApiKey(apiKey)) {
			AttributeInfo[] response = new AttributeInfo[1];
			response[0] = new AttributeInfo("No catalog available","Operation not allowed for this user. Please check your API key.");
			return new ResponseEntity<>(response, HttpStatus.FORBIDDEN);
		}

		// Identify the coordinator that handles data sources for the specified API key
		Coordinator myCoordinator = dictCoordinators.get(apiKey);
		
		System.out.println("Listing available data sources...");

		// CATALOG OF DATA SOURCES
		try {				
			// Invoke listing of available data sources
			return new ResponseEntity<>(myCoordinator.listDataSources(params), HttpStatus.OK);			
		}
		catch (Exception e) {
			e.printStackTrace();
			System.out.println("Listing of data sources terminated abnormally.");
			AttributeInfo[] response = new AttributeInfo[1];
			response[0] = new AttributeInfo("No catalog available","Listing of data sources terminated abnormally");
			return new ResponseEntity<>(response, HttpStatus.NOT_FOUND);	
		}
	}


	/**
	 * Removes attribute(s) from those available for similarity search queries.
	 * Any removed attribute(s) can become available again with an index request to the RESTful service.
	 * @param apiKey  The client API key associated with the corresponding data sources.
	 * @param params  Parameters specified in JSON (instantiating a RemoveRequest object) that declare the attribute(s) to become unavailable for queries.
	 * @return  A notification on whether the requested attribute removal succeeded or not.
	 */
	@CrossOrigin
	@RequestMapping(value = "/delete", method = { RequestMethod.POST }, consumes = "application/json", produces = "application/json")
	public ResponseEntity<Response> delete(@ApiParam("The client API key allowing access to the data") @RequestHeader("api_key") String apiKey, @ApiParam("Request to method delete") @RequestBody RemoveRequest params) {

		Response delResponse;
		
		if (!isValidApiKey(apiKey)) {
			delResponse = new Response();
			delResponse.setNotification("Operation not allowed for this user. Please check your API key.");
			return new ResponseEntity<>(delResponse, HttpStatus.FORBIDDEN);
		}

		// Identify the coordinator that handles data sources for the specified API key
		Coordinator myCoordinator = dictCoordinators.get(apiKey);

		// REMOVAL OF ATTRIBUTE DATA
		try {
			// Invoke removal functionality
			delResponse = myCoordinator.delete(params);
			delResponse.appendNotification("Any maintained indices have been purged.");
			myCoordinator.log("Removed the specified data source(s) associated with this API key: " + apiKey);
			System.out.println("Attribute removal completed!");			
		}
		catch (NullPointerException e) {
			e.printStackTrace();
			delResponse = new Response();
			delResponse.setNotification("No dataset with at least one of the specified attributes is available for search. Make sure that the JSON file provides suitable specifications.");
			System.out.println(delResponse.getNotification());
			return new ResponseEntity<>(delResponse, HttpStatus.BAD_REQUEST);
		}
		catch (Exception e) {
			e.printStackTrace();
			delResponse = new Response();
			delResponse.setNotification("Attribute data removal terminated abnormally. Make sure that the submitted JSON configuration provides suitable specifications.");
			System.out.println(delResponse.getNotification());
			return new ResponseEntity<>(delResponse, HttpStatus.BAD_REQUEST);
		}

		return new ResponseEntity<>(delResponse, HttpStatus.OK);
	}


	/**
	 * Allows submission or multi-attribute similarity search requests to the RESTful service.
	 * @param apiKey  The client API key associated with the corresponding data sources.
	 * @param params  Parameters specified in JSON (instantiating a SearchRequest object) that define the attributes, query values, and weights to be applied in the search request.
	 * @return  A JSON with the ranked results qualifying to the criteria specified in the similarity search query.
	 */
	@CrossOrigin
	@RequestMapping(value = "/search", method = { RequestMethod.POST }, consumes = "application/json", produces = "application/json")
	public ResponseEntity<SearchResponse[]> search(@ApiParam("The client API key") @RequestHeader("api_key") String apiKey, @ApiParam("Request to method search") @RequestBody SearchRequest params) {

		if (!isValidApiKey(apiKey)) {
			SearchResponse[] response = new SearchResponse[1];
			SearchResponse res0 = new SearchResponse();
			res0.setNotification("Operation not allowed for this user. Please check your API key.");
			response[0] = res0;
			return new ResponseEntity<>(response, HttpStatus.FORBIDDEN);
		}

		// Identify the coordinator that handles data sources for the specified API key
		Coordinator myCoordinator = dictCoordinators.get(apiKey);
				
		// SEARCH
		try {	
			// Invoke search with the parameters specified in the query configuration
			return new ResponseEntity<>(myCoordinator.search(params), HttpStatus.OK);			
		}
		catch (Exception e) {
			e.printStackTrace();
			System.out.println("Query evaluation terminated abnormally.");
			SearchResponse[] response = new SearchResponse[1];
			SearchResponse res0 = new SearchResponse();
			res0.setNotification("Query evaluation terminated abnormally. Make sure that the submitted JSON configuration provides suitable query specifications.");
			response[0] = res0;
			return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);	
		}
	}

}
