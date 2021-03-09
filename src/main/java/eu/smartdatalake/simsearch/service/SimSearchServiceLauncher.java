package eu.smartdatalake.simsearch.service;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.springframework.boot.SpringApplication;

/**
 * Entry point to the RESTful web service that invokes a Spring boot application.
 */
public class SimSearchServiceLauncher {

	private String getKeys(JSONObject apiKeys, String group) {
		
		String keysString = "";
		JSONArray keys = (JSONArray) apiKeys.get(group);		
		for (Object key : keys) {
			keysString += key + " ";
		}
		keysString = keysString.trim();
		
		return keysString;
	}
	
	/**
	 * Main entry point to the launch the RESTful service.
	 * @param args Optionally, the path to the JSON specifying the various API keys.
	 * @throws ParseException 
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public SimSearchServiceLauncher(String[] args) throws FileNotFoundException, IOException, ParseException {
		
		// Load admin-defined API keys, if specified after the --service directive
		if (args.length > 1) {
			String apiKeysFile = args.length > 1 ? args[1] : "valid_api_keys.json";
	
			JSONParser jsonParser = new JSONParser();
			JSONObject apiKeys = (JSONObject) jsonParser.parse(new FileReader(apiKeysFile));
			
			System.setProperty("admin_api_keys", getKeys(apiKeys, "admin_api_keys"));
		}
		
		// Start the service
		SpringApplication.run(SimSearchServiceApplication.class, args);
	}
	
}
