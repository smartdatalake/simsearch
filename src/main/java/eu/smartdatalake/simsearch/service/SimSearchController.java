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
import eu.smartdatalake.simsearch.SearchResponse;
import eu.smartdatalake.simsearch.AttributeInfo;
import eu.smartdatalake.simsearch.request.MountRequest;
import eu.smartdatalake.simsearch.request.CatalogRequest;
import eu.smartdatalake.simsearch.request.RemoveRequest;
import eu.smartdatalake.simsearch.request.SearchRequest;

import java.lang.Exception;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Controller that handles all HTTP requests to the RESTful web service.
 */
@RestController
@RequestMapping("/simsearch/api")
public class SimSearchController {

	Coordinator myCoordinator;    	// The coordinator of the SimSearch back-end.
	List<String> adminApiKeys;		// List of API keys offering support for administrative requests (add/remove attributes), as well as catalog and search requests.
	List<String> userApiKeys;		// List of API keys offering support for catalog and search requests.


	/**
	 * Instantiates the valid API keys that control access to the service.
	 * @param propKeys  The name of the system property that holds the API keys as read from a JSON file.
	 * @return The list of valid API keys.
	 */
	private List<String> getApiKeys(String propKeys) {

		List<String> validApiKeys = new ArrayList<String>();

		String keysString = System.getProperty(propKeys);
		if (keysString != null ) {
			StringTokenizer strTok = new StringTokenizer(keysString, " ");   // System property uses a blank space as delimiter between API keys.
			while (strTok.hasMoreTokens()) {
				validApiKeys.add(strTok.nextToken());
			}
		}
		
		return validApiKeys;
	}
	
	/**
	 * Checks whether the given API key is valid, i.e., listed among those specified by the service administrator.
	 * @param api_key  The API key to check.
	 * @return  True, if the key is valid; otherwise, False.
	 */
	private boolean isValidApiKey(String api_key) {

		return userApiKeys.contains(api_key) ? true : false;
	}
	
	/**
	 * Checks whether the given API key provides administrative privileges, i.e., it allows adding or deleting data sources.
	 * @param api_key  The API key to check.
	 * @return  True, if the key provides administrative privileges; otherwise, False.
	 */
	private boolean isAdminApiKey(String api_key) {

		return adminApiKeys.contains(api_key) ? true : false;
	}
	
	/**
	 * Constructor. It also instantiates the lists of API keys.
	 */
	@Autowired
	public SimSearchController() {

		myCoordinator = new Coordinator();

		// Instantiate the API keys specified in the input JSON for each group of users
		adminApiKeys = getApiKeys("admin_api_keys");
		userApiKeys = getApiKeys("user_api_keys");
	}


	/**
	 * Mounts the specified data sources, including building in-memory indices or instantiating connections to remote sources. 
	 * @param apiKey  The client API key providing administrative privileges.
	 * @param params  Parameters specified in JSON (instantiating a MountRequest object) that specify the data sources that will be enabled and their queryable attributes. 
	 * @return  A notification whether the data sources were successfully maounted or not.
	 */
	@RequestMapping(value = "/index", method = { RequestMethod.POST }, consumes = "application/json", produces = "application/json")
	public ResponseEntity<String> index(@ApiParam("The client API key providing administrative privileges") @RequestHeader("api_key") String apiKey, @ApiParam("Request to method index") @RequestBody MountRequest params) {

		if (!isAdminApiKey(apiKey)) {
			return new ResponseEntity<>("Operation not allowed for this user. Please check if your API key.", HttpStatus.FORBIDDEN);
		}

		System.out.println("Preparing indices. This process may take a few minutes. Please wait...");

		// MOUNTING
		try {	
			// Invoke mounting step
			myCoordinator.mount(params);
			System.out.println("Mounting of data sources completed!");			
		}
		catch (Exception e) {
			e.printStackTrace();
			System.out.println("Mounting stage terminated abnormally.");
			return new ResponseEntity<>("Mounting stage terminated abnormally. Make sure that the submitted JSON configuration provides suitable specifications.", HttpStatus.BAD_REQUEST);
		}

		return new ResponseEntity<>("Mounting of data sources completed successfully. This data is now available for search requests.", HttpStatus.OK);
	}


	/**
	 * Lists the queryable attributes that may be included in similarity search requests.
	 * @param apiKey  The client API key (no administrative privileges required).
	 * @param params Parameters specified in JSON (instantiating a CatalogRequest object); if empty {}, then all sources will be listed. 
	 * @return  A JSON listing the catalog of queryable attributes and the similarity operation supported by each one.
	 */
	@CrossOrigin
	@RequestMapping(value = "/catalog", method = { RequestMethod.POST }, consumes = "application/json", produces = "application/json")
	public ResponseEntity<AttributeInfo[]> catalog(@ApiParam("The client API key") @RequestHeader("api_key") String apiKey, @ApiParam("Request to method index") @RequestBody CatalogRequest params) {

		if (!isValidApiKey(apiKey)) {
			AttributeInfo[] response = new AttributeInfo[1];
			response[0] = new AttributeInfo("No catalog available","Operation not allowed for this user. Please check if your API key.");
			return new ResponseEntity<>(response, HttpStatus.FORBIDDEN);
		}

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
	 * @param apiKey  The client API key providing administrative privileges.
	 * @param params  Parameters specified in JSON (instantiating a RemoveRequest object) that declare the attribute(s) to become unavailable for queries.
	 * @return  A notification on whether the requested attribute removal succeeded or not.
	 */
	@RequestMapping(value = "/delete", method = { RequestMethod.POST }, consumes = "application/json", produces = "application/json")
	public ResponseEntity<String> delete(@ApiParam("The client API key providing administrative privileges") @RequestHeader("api_key") String apiKey, @ApiParam("Request to method delete") @RequestBody RemoveRequest params) {

		if (!isAdminApiKey(apiKey)) {
			return new ResponseEntity<>("Operation not allowed for this user. Please check if your API key.", HttpStatus.FORBIDDEN);
		}

		System.out.println("Removing data sources and any related indices. Please wait...");

		// REMOVAL OF ATTRIBUTE DATASET
		try {
			// Invoke removal functionality
			myCoordinator.delete(params);
			System.out.println("Attribute removal completed!");			
		}
		catch (NullPointerException e) {
			e.printStackTrace();
			System.out.println("No dataset with at least one of the specified attributes is available for search. Make sure that the JSON file provides suitable specifications.");
			return new ResponseEntity<>("No dataset with at least one of the specified attributes is available for search. Make sure that the submitted JSON configuration provides suitable specifications.", HttpStatus.BAD_REQUEST);
		}
		catch (Exception e) {
			e.printStackTrace();
			System.out.println("Attribute data removal terminated abnormally. Make sure that the submitted JSON configuration provides suitable specifications.");
			return new ResponseEntity<>("Attribute data removal terminated abnormally. Make sure that the submitted JSON configuration provides suitable specifications.", HttpStatus.BAD_REQUEST);
		}

		return new ResponseEntity<>("Attribute dataset(s) removed from available sources. Any maintained indices have been purged.", HttpStatus.OK);
	}


	/**
	 * Allows submission or multi-attribute similarity search requests to the RESTful service.
	 * @param apiKey  The client API key (no administrative privileges required).
	 * @param params  Parameters specified in JSON (instantiating a SearchRequest object) that define the attributes, query values, and weights to be applied in the search request.
	 * @return  A JSON with the ranked results qualifying to the specified criteria.
	 */
	@CrossOrigin
	@RequestMapping(value = "/search", method = { RequestMethod.POST }, consumes = "application/json", produces = "application/json")
	public ResponseEntity<SearchResponse[]> search(@ApiParam("The client API key") @RequestHeader("api_key") String apiKey, @ApiParam("Request to method search") @RequestBody SearchRequest params) {

		if (!isValidApiKey(apiKey)) {
			SearchResponse[] response = new SearchResponse[1];
			SearchResponse res0 = new SearchResponse();
			res0.setNotification("Operation not allowed for this user. Please check if your API key.");
			response[0] = res0;
			return new ResponseEntity<>(response, HttpStatus.FORBIDDEN);
		}

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
