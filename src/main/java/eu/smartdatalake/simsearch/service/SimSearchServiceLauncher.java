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

	/**
	 * Concatenates the API keys in a string for in-memory storage in a system property.
	 * @param apiKeys  JSON containing the various types of API keys.
	 * @param group  Specifies which group of API keys (admin/user) to read from JSON.
	 * @return  A concatenated string composed of the API keys read.
	 */
	private String getKeys(JSONObject apiKeys, String group) {
				
		String keysString = "";
		JSONArray keys = (JSONArray) apiKeys.get(group);		
		for (Object key : keys) {
			keysString += key + " ";   // A blank character is used as a delimiter between the API keys
		}
		keysString = keysString.trim();
		
		return keysString;
	}
	
	/**
	 * Main entry point to the launch the RESTful service.
	 * @param args Optionally, the path to the JSON specifying the various API keys.
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws ParseException
	 */
	public SimSearchServiceLauncher(String[] args) throws FileNotFoundException, IOException, ParseException {

		// Load valid API keys in order to control access to the service
		String apiKeysFile = args.length > 1 ? args[1] : "valid_api_keys.json";

		JSONParser jsonParser = new JSONParser();
		JSONObject apiKeys = (JSONObject) jsonParser.parse(new FileReader(apiKeysFile));
		
		System.setProperty("admin_api_keys", getKeys(apiKeys, "admin_api_keys"));
		System.setProperty("user_api_keys", getKeys(apiKeys, "user_api_keys"));
		
		// Start the service
		SpringApplication.run(SimSearchServiceApplication.class, args);
	}
}
