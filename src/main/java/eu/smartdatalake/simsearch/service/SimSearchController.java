package eu.smartdatalake.simsearch.service;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.CrossOrigin;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import eu.smartdatalake.simsearch.Coordinator;
import eu.smartdatalake.simsearch.InstanceSettings;
import eu.smartdatalake.simsearch.engine.Response;
import eu.smartdatalake.simsearch.engine.SearchResponse;
import eu.smartdatalake.simsearch.manager.AttributeInfo;
import eu.smartdatalake.simsearch.request.MountRequest;
import eu.smartdatalake.simsearch.request.CatalogRequest;
import eu.smartdatalake.simsearch.request.RemoveRequest;
import eu.smartdatalake.simsearch.request.SearchRequest;

import java.lang.Exception;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.xml.bind.DatatypeConverter;

/**
 * Controller that handles all HTTP requests to the RESTful web service.
 */
@RestController
@RequestMapping("/simsearch/api")
public class SimSearchController {

	int API_key_length = 128;   		// Standard length (in bits) for all generated API keys
	
	Map<String, Coordinator> dictCoordinators;	// Dictionary of all coordinator instances (each with its own API key) deployed at the SimSearch back-end.

	Map<String, Set<String>> extraApiKeys;		// For a "master" API key, keep a set of any associated API keys available for search and catalog operations
	
	List<String> adminApiKeys;   		// List of admin-defined API keys

	/**
	 * Provides the list of API keys defined by the administrator upon launching of the service.
	 * @param propKeys  A string containing comma-separated values representing admin-defined API keys.
	 * @return  A list of API keys as defined by the administrator when the service is launched.
	 */
	private List<String> getApiKeys(String propKeys) {

		List<String> validApiKeys = new ArrayList<String>();

		String keysString = System.getProperty(propKeys);
		if (keysString != null ) {
			StringTokenizer strTok = new StringTokenizer(keysString, " ");
			while (strTok.hasMoreTokens()) {
				validApiKeys.add(strTok.nextToken());
			}
		}

		return validApiKeys;
	}

	
	/**
	 * Checks whether the given API key is defined by the administrator, i.e., using a JSON when the service is launched.
	 * @param api_key  The API key to check.
	 * @return  A Boolean value: True, if the key is defined by the administrator; otherwise, False.
	 */
	private boolean isAdminApiKey(String api_key) {

		return adminApiKeys.contains(api_key) ? true : false;
	}
	
	
	/**
	 * Checks whether the given API key is valid, i.e., listed among those generated for the various data sources.
	 * @param api_key  The API key to check.
	 * @return  A Boolean value: True, if the key is valid; otherwise, False.
	 */
	private boolean isValidApiKey(String api_key) {

		return dictCoordinators.containsKey(api_key) ? true : false;
	}

	
	/**
	 * Checks whether the given API key is associated with a master API key and thus allows certain requests (search, catalog) against the respective datasets.
	 * @param api_key  The API key to check.
	 * @return  The master API key to employ for evaluating any requests.
	 */
	private String getMasterApiKey(String api_key) {
		
		for (String masterApiKey: extraApiKeys.keySet()) {
			if (extraApiKeys.get(masterApiKey).contains(api_key))
				return masterApiKey;   // The master API key to use for requests to the data sources  
		}
		
		return api_key;   // There is no associated master API key
	}
	
	/**
	 * Associates the given extra API key to an existing master API key that enables certain requests (catalog, search).
	 * @param masterApiKey  The master API key that enables SimSearch requests.
	 * @param extraApiKey  The extra API key to associate with the master API key.
	 * @return  A boolean value: True, if the extra API key has been associated with the master API key; otherwise, False.
	 */
	private boolean assignExtraApiKey(String masterApiKey, String extraApiKey) {
		
		// Check if extra API key is already in use, even for another master API key
		for (String key: extraApiKeys.keySet()) {
			if (extraApiKeys.get(key).contains(extraApiKey))
				return false;     
		}
		
		// If the master API key has no associated extra key, initialize its set
		if (!extraApiKeys.containsKey(masterApiKey))
			extraApiKeys.put(masterApiKey, new HashSet<String>());
			
		// Associate the extra API key to the master API key
		return extraApiKeys.get(masterApiKey).add(extraApiKey);

	}
	
	/**
	 * Revokes and deactivates the given extra API key from an existing master API key; specifying requests with the extra API key will be no longer enabled.
	 * @param masterApiKey  The master API key that controls SimSearch requests.
	 * @param extraApiKey  The extra API key to revoke from the master API key.
	 * @return  A boolean value: True, if the extra API key has been revoked; otherwise, False.
	 */
	private boolean revokeExtraApiKey(String masterApiKey, String extraApiKey) {
		
		if (extraApiKeys.containsKey(masterApiKey))
			return extraApiKeys.get(masterApiKey).remove(extraApiKey);
			
		return false;

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
		
		// Instantiate any administrative API keys specified in the optional input JSON
		// This specifies the admin-defined API keys
		adminApiKeys = getApiKeys("admin_api_keys");
		
		// Instantiate dictionary to keep any extra API keys
		extraApiKeys = new HashMap<String, Set<String>>();
	}

	
	/**
	 * Provides general information about the running SimSearch service accessible through this API key.
	 * @param apiKey  The client API key controlling the corresponding data sources.
	 * @return  The main settings of the running instance.
	 */
	@CrossOrigin
	@ApiOperation(value = "Provides general information about the running SimSearch service accessible through this API key")
	@RequestMapping(value = "/_settings", method = { RequestMethod.POST, RequestMethod.GET }, consumes = "application/json", produces = "application/json")
	public ResponseEntity<JSONObject> settings(@ApiParam("The client API key allowing access to the data") @RequestHeader("api_key") String apiKey) {
		
		// Identify the master API key, if applicable 
		apiKey = getMasterApiKey(apiKey);  // value may change to its master API key
				
		if (!isValidApiKey(apiKey)) {
			JSONObject res = new JSONObject();
			res.put("Notification", "Operation not allowed for this user. Please check your client API key.");
			return new ResponseEntity<>(res, HttpStatus.FORBIDDEN);
		}
		else {
			// Identify the coordinator that handles existing data sources for this API key
			Coordinator myCoordinator = dictCoordinators.get(apiKey);
			InstanceSettings settings = myCoordinator.getSettings();
			JSONObject res = new JSONObject();
			res.put(settings.settings.index.getProvidedName(), settings);	
			return new ResponseEntity<>(res, HttpStatus.OK);
		}	
	}
	
	
	/**
	 * Associates an API key with a master API key, thus enabling certain requests (catalog, search).
	 * @param apiKey  The client API key controlling the corresponding data sources.
	 * @param extraKey  The extra API key to associate.
	 * @return  A notification to the user.
	 */
	@CrossOrigin
	@ApiOperation(value = "Associate an extra API key with the given client API key; certain requests (catalog, search) controlled by the client API key will be also allowed with this extra key")
	@RequestMapping(value = "/assignKey", method = { RequestMethod.POST }, consumes = "application/json", produces = "application/json")
	public ResponseEntity<Response> assignKey(@ApiParam("The client API key allowing access to the data") @RequestHeader("api_key") String apiKey, @ApiParam("The extra API key to associate with this client API key") @RequestBody String extraKey) {
		
		Response assignResponse = new Response();

		if (!isValidApiKey(apiKey)) {
			assignResponse.setNotification("Operation not allowed for this user. Please check your client API key.");
			return new ResponseEntity<>(assignResponse, HttpStatus.FORBIDDEN);
		}
		
		if (assignExtraApiKey(apiKey, extraKey)) {
			// Identify the coordinator that handles existing data sources for the master API key
			Coordinator myCoordinator = dictCoordinators.get(apiKey);
			assignResponse.setNotification("Data sources controlled by API key " + apiKey + " can now be also queried using this API key: " + extraKey);
			assignResponse.setApiKey(extraKey);
			myCoordinator.log("Data sources controlled by API key " + apiKey + " can now be also queried using this API key: " + extraKey);
			return new ResponseEntity<>(assignResponse, HttpStatus.OK);
		}
		else {
			assignResponse.setNotification("Operation failed. Extra API key is already associated with a client API key. Please check your API keys.");
			return new ResponseEntity<>(assignResponse, HttpStatus.BAD_REQUEST);
		}	
	}
	
	
	/**
	 * Removes an API key that has been associated with a master API key, which controls similarity search requests.
	 * @param apiKey  The client API key controlling the corresponding data sources.
	 * @param extraKey  The extra API key to revoke.
	 * @return  A notification to the user.
	 */
	@CrossOrigin
	@ApiOperation(value = "Revokes the specified extra API key associated with the client API key; the revoked API key can no loger be used in similarity search requests")
	@RequestMapping(value = "/revokeKey", method = { RequestMethod.POST }, consumes = "application/json", produces = "application/json")
	public ResponseEntity<Response> revokeKey(@ApiParam("The client API key allowing access to the data") @RequestHeader("api_key") String apiKey, @ApiParam("The extra API key to deactivate") @RequestBody String extraKey) {
		
		Response revokeResponse = new Response();

		if (!isValidApiKey(apiKey)) {
			revokeResponse.setNotification("Operation not allowed for this user. Please check your client API key.");
			return new ResponseEntity<>(revokeResponse, HttpStatus.FORBIDDEN);
		}
		
		if (revokeExtraApiKey(apiKey, extraKey)) {
			// Identify the coordinator that handles existing data sources for the master API key
			Coordinator myCoordinator = dictCoordinators.get(apiKey);
			revokeResponse.setNotification("Data sources controlled by API key " + apiKey + " are no longer available for queries using this API key: " + extraKey);
			revokeResponse.setApiKey(extraKey);
			myCoordinator.log("Data sources controlled by API key " + apiKey + " are no longer available for queries using this API key: " + extraKey);
			return new ResponseEntity<>(revokeResponse, HttpStatus.OK);
		}
		else {
			revokeResponse.setNotification("Operation failed. There is no such extra API key associated with the given client API key. Please check your API keys.");
			return new ResponseEntity<>(revokeResponse, HttpStatus.BAD_REQUEST);
		}	
	}
	

	/**
	 * Lists any API keys associated with a master API key that controls similarity search requests.
	 * @param apiKey  The client API key controlling the corresponding data sources.
	 * @return  A JSON listing the catalog of queryable attributes and the similarity operation supported by each one.
	 */
	@CrossOrigin
	@ApiOperation(value = "List any extra API keys associated with the given client API key that enables similarity search requests")
	@RequestMapping(value = "/listKeys", method = { RequestMethod.POST }, consumes = "application/json", produces = "application/json")
	public ResponseEntity<Response> listKeys(@ApiParam("The client API key") @RequestHeader("api_key") String apiKey) {

		Response listResponse = new Response();
		
		if (!isValidApiKey(apiKey)) {
			listResponse.setNotification("Operation not allowed for this user. Please check your API key.");
			return new ResponseEntity<>(listResponse, HttpStatus.FORBIDDEN);
		}

		// Report any associated keys
		if (extraApiKeys.containsKey(apiKey)) {
			String[] extraKeys = extraApiKeys.get(apiKey).toArray(new String[0]);
			if (extraKeys.length > 0) {
				listResponse.setNotification("API key " + apiKey + " has the following associated extra keys: " + Arrays.toString(extraKeys));
				return new ResponseEntity<>(listResponse, HttpStatus.OK);
			}
		}

		// No associated keys found
		listResponse.setNotification("API key " + apiKey + " has no associated extra keys.");
		return new ResponseEntity<>(listResponse, HttpStatus.BAD_REQUEST);	
	}

	
	/**
	 * Mounts the specified data sources and associates them to a new API key, including building in-memory indices or instantiating connections to remote sources. 
	 * @param params  Parameters specified in JSON (instantiating a MountRequest object) that specify the data sources that will be enabled and their queryable attributes. 
	 * @return  A notification whether the data sources were successfully mounted or not. If successful, this request also returns an API key to be used in all subsequent requests involving these data sources.
	 */
	@CrossOrigin
	@ApiOperation(value = "Mount the specified data sources and associate them to a new API key, including building in-memory indices or instantiating connections to remote sources")
	@RequestMapping(value = "/index", method = { RequestMethod.POST }, consumes = "application/json", produces = "application/json")
	public ResponseEntity<Response> index(@ApiParam("Parameters in this request") @RequestBody MountRequest params) {

		Response mountResponse;
		
		// Create a new instance of the coordinator at the SimSearch back-end
		Coordinator myCoordinator = new Coordinator();
		// A new API key where the specified data source(s) will be assigned to
		String apiKey;

//		System.out.println("Preparing indices. This process may take a few minutes. Please wait...");

		// MOUNTING DATA SOURCE(S)
		try {	
			// Invoke mounting step
			mountResponse = myCoordinator.mount(params);
			// Generate a new API key ...
			apiKey = generateAPIkey();
			// ... that must be used in all subsequent requests against these data sources
			dictCoordinators.put(apiKey, myCoordinator);
			// In case of no errors, notify accordingly
			if (mountResponse.getNotification() != null) 
				mountResponse.appendNotification("Mounting stage terminated abnormally. Some data sources may not have been mounted.");
			else
				mountResponse.appendNotification("Mounting of data sources completed successfully.");
			mountResponse.appendNotification("***IMPORTANT*** In all subsequent requests regarding this data, you must specify this API key: " + apiKey);
			mountResponse.setApiKey(apiKey);
			myCoordinator.log("Newly created data sources have been associated with this API key: " + apiKey);			
		}
		catch (Exception e) {
			e.printStackTrace();
			mountResponse = new Response();
			mountResponse.setNotification("Mounting stage terminated abnormally. Make sure that the submitted JSON configuration provides suitable specifications.");
//			System.out.println("Mounting stage terminated abnormally.");
			return new ResponseEntity<>(mountResponse, HttpStatus.BAD_REQUEST);
		}

//		System.out.println("Mounting of data sources completed!");
		return new ResponseEntity<>(mountResponse, HttpStatus.OK);
	}
	

	/**
	 * Appends the specified data sources to those already mounted for this API key, including building in-memory indices or instantiating connections to remote sources. 
	 * @param apiKey  The client API key associated with the corresponding data sources.
	 * @param params  Parameters specified in JSON (instantiating a MountRequest object) that specify the data sources that will be enabled and their queryable attributes. 
	 * @return  A notification whether the data sources were successfully mounted or not.
	 */
	@CrossOrigin
	@ApiOperation(value = "Append the specified data sources to those already mounted under the given API key, including building in-memory indices or instantiating connections to remote sources")
	@RequestMapping(value = "/append", method = { RequestMethod.POST }, consumes = "application/json", produces = "application/json")
	public ResponseEntity<Response> append(@ApiParam("The client API key allowing access to the data") @RequestHeader("api_key") String apiKey, @ApiParam("Parameters in this request") @RequestBody MountRequest params) {

		Response appendResponse;

		if (!isValidApiKey(apiKey) && !isAdminApiKey(apiKey)) {
			appendResponse = new Response();
			appendResponse.setNotification("Operation not allowed for this user. Please check your API key.");
			return new ResponseEntity<>(appendResponse, HttpStatus.FORBIDDEN);
		}
		
		// Identify the coordinator that handles existing data sources for the specified API key
		Coordinator myCoordinator = dictCoordinators.get(apiKey);

		// If no coordinator exists yet for admin-defined API key, create one...
		if ((isAdminApiKey(apiKey)) && (myCoordinator == null)) {
			 myCoordinator = new Coordinator();
			// ... and keep it for use in all subsequent requests against these data sources
			dictCoordinators.put(apiKey, myCoordinator);
		}
		
//		System.out.println("Preparing indices. This process may take a few minutes. Please wait...");

		// MOUNTING DATA SOURCE(S)
		try {	
			// Invoke mounting step
			appendResponse = myCoordinator.mount(params);
			// In case of no errors, notify accordingly
			if (appendResponse.getNotification() != null) 
				appendResponse.appendNotification("Mounting stage terminated abnormally. Some data sources may not have been appended.");
			appendResponse.appendNotification("Appended data sources are now available for queries and are associated with this API key: " + apiKey);
			appendResponse.setApiKey(apiKey);
			myCoordinator.log("Appended data sources are now available for queries and are associated with this API key: " + apiKey);			
		}
		catch (Exception e) {
			e.printStackTrace();
			appendResponse = new Response();
			appendResponse.setNotification("Mounting stage terminated abnormally. Make sure that the submitted JSON configuration provides suitable specifications.");
//			System.out.println("Mounting stage terminated abnormally.");
			return new ResponseEntity<>(appendResponse, HttpStatus.BAD_REQUEST);
		}

//		System.out.println("Mounting of data sources completed!");
		return new ResponseEntity<>(appendResponse, HttpStatus.OK);
	}


	/**
	 * Unmounts all data sources associated with the given API key, and destroys the corresponding instance of SimSearch. 
	 * @param apiKey  The client API key associated with a running instance of SimSearch service.
	 * @return  A notification about the status of the SimSearch instance.
	 */
	@CrossOrigin
	@ApiOperation(value = "Unmount all data sources associated with the given API key; the corresponding instance of SimSearch will be destroyed and can no longer respond to requests")
	@RequestMapping(value = "/unmount", method = { RequestMethod.POST }, consumes = "application/json", produces = "application/json")
	public ResponseEntity<Response> unmount(@ApiParam("The client API key allowing access to the data") @RequestHeader("api_key") String apiKey) {

		Response unmountResponse = new Response();
		
		if (!isValidApiKey(apiKey)) {
			unmountResponse.setNotification("Operation not allowed for this user. Please check your API key.");
			return new ResponseEntity<>(unmountResponse, HttpStatus.FORBIDDEN);
		}

		// Dismiss the coordinator corresponding to the specified API key
		Coordinator myCoordinator = dictCoordinators.get(apiKey);
		if (myCoordinator != null) {
			myCoordinator.log("********************** Unmounting data sources ... **********************");
			myCoordinator.log("SimSearch instance controlled by API key " + apiKey + " is no longer mounted and cannot support any requests.");
			myCoordinator = null;
			dictCoordinators.remove(apiKey);
			extraApiKeys.remove(apiKey);
			unmountResponse.setNotification("SimSearch instance controlled by API key " + apiKey + " is no longer mounted and cannot support any requests. Any associated API keys have been deleted.");
			return new ResponseEntity<>(unmountResponse, HttpStatus.OK);
		}
		else {
			unmountResponse.setNotification("Cannot find any instance of SimSearch associated with API key " + apiKey + ". Please check your API key.");
			return new ResponseEntity<>(unmountResponse, HttpStatus.BAD_REQUEST);
		}
	}

	
	/**
	 * Lists the queryable attributes that may be included in similarity search requests.
	 * @param apiKey  The client API key associated with the corresponding data sources.
	 * @param params Parameters specified in JSON (instantiating a CatalogRequest object); if empty {}, then all sources will be listed. 
	 * @return  A JSON listing the catalog of queryable attributes and the similarity operation supported by each one.
	 */
	@CrossOrigin
	@ApiOperation(value = "List the queryable attributes associated with the given client API key; these may be included in similarity search requests")
	@RequestMapping(value = "/catalog", method = { RequestMethod.POST }, consumes = "application/json", produces = "application/json")
	public ResponseEntity<AttributeInfo[]> catalog(@ApiParam("The client API key") @RequestHeader("api_key") String apiKey, @ApiParam("Parameters in this request") @RequestBody CatalogRequest params) {

		// Identify the master API key, if applicable 
		apiKey = getMasterApiKey(apiKey);  // value may change to its master API key
			
		if (!isValidApiKey(apiKey)) {
			AttributeInfo[] response = new AttributeInfo[1];
			response[0] = new AttributeInfo("No catalog available","Operation not allowed for this user. Please check your API key.");
			return new ResponseEntity<>(response, HttpStatus.FORBIDDEN);
		}

		// Identify the coordinator that handles data sources for the specified API key
		Coordinator myCoordinator = dictCoordinators.get(apiKey);

		// CATALOG OF DATA SOURCES
		try {				
			// Invoke listing of available data sources
			return new ResponseEntity<>(myCoordinator.listDataSources(params), HttpStatus.OK);			
		}
		catch (Exception e) {
			e.printStackTrace();
//			System.out.println("Listing of data sources terminated abnormally.");
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
	@ApiOperation(value = "Remove attribute(s) associated with the given client API key; these become no longer queryable in similarity search queries")
	@RequestMapping(value = "/delete", method = { RequestMethod.POST }, consumes = "application/json", produces = "application/json")
	public ResponseEntity<Response> delete(@ApiParam("The client API key allowing access to the data") @RequestHeader("api_key") String apiKey, @ApiParam("Parameters in this request") @RequestBody RemoveRequest params) {

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
		}
		catch (NullPointerException e) {
			e.printStackTrace();
			delResponse = new Response();
			delResponse.setNotification("No dataset with at least one of the specified attributes is available for search. Make sure that the JSON file provides suitable specifications.");
			return new ResponseEntity<>(delResponse, HttpStatus.BAD_REQUEST);
		}
		catch (Exception e) {
			e.printStackTrace();
			delResponse = new Response();
			delResponse.setNotification("Attribute data removal terminated abnormally. Make sure that the submitted JSON configuration provides suitable specifications.");
			return new ResponseEntity<>(delResponse, HttpStatus.BAD_REQUEST);
		}

		return new ResponseEntity<>(delResponse, HttpStatus.OK);
	}


	/**
	 * Allows submission of multi-attribute similarity search requests to the RESTful service.
	 * @param apiKey  The client API key associated with the corresponding data sources.
	 * @param params  Parameters specified in JSON (instantiating a SearchRequest object) that define the attributes, query values, and weights to be applied in the search request.
	 * @return  A JSON with the ranked results qualifying to the criteria specified in the similarity search query.
	 */
	@CrossOrigin
	@ApiOperation(value = "Submit multi-attribute similarity search requests with user-specified query values and parameters")
	@RequestMapping(value = "/search", method = { RequestMethod.POST }, consumes = "application/json", produces = "application/json")
	public ResponseEntity<SearchResponse[]> search(@ApiParam("The client API key") @RequestHeader("api_key") String apiKey, @ApiParam("Parameters in this request") @RequestBody SearchRequest params) {

		// Identify the master API key, if applicable 
		apiKey = getMasterApiKey(apiKey);  // value may change to its master API key
		
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
			SearchResponse[] response = new SearchResponse[1];
			SearchResponse res0 = new SearchResponse();
			res0.setNotification("Query evaluation terminated abnormally. Make sure that the submitted JSON configuration provides suitable query specifications.");
			response[0] = res0;
			return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);	
		}
	}

}
