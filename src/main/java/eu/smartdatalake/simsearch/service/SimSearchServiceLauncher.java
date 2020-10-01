package eu.smartdatalake.simsearch.service;

import org.springframework.boot.SpringApplication;

/**
 * Entry point to the RESTful web service that invokes a Spring boot application.
 */
public class SimSearchServiceLauncher {

	/**
	 * Main entry point to the launch the RESTful service.
	 * @param args Optionally, the path to the JSON specifying the various API keys.
	 */
	public SimSearchServiceLauncher(String[] args) {
		
		// Start the service
		SpringApplication.run(SimSearchServiceApplication.class, args);
	}
}
