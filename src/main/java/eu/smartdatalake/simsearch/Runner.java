package eu.smartdatalake.simsearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import eu.smartdatalake.simsearch.service.SimSearchServiceLauncher;

/**
 * Main class for invoking execution of a multi-attribute similarity search request that issues ranked top-k results with aggregate scores.
 * Before issuing any queries, attribute data sources must be mounted (either ingested from CSV files or in-situ from JDBC connections or REST APIs) using a JSON configuration.
 * Query configurations (specified in JSON) can be submitted once data sources have been mounted. 
 * 
 * Execution command -- STANDALONE mode with requests specified from standard input: 
 * java -jar target/simsearch-0.0.1-SNAPSHOT.jar
 * 
 * Execution command -- SERVICE mode: for launching a web service, e.g., at port 8090, and specifying requests using the REST API: 
 * java -Dserver.port=8090 -jar target/simsearch-0.0.1-SNAPSHOT.jar --service
 */
public class Runner {	
	
	/**
	 * Provides the path to the JSON file containing configurations for the various requests.
	 * @return  Path to a JSON file.
	 */
	private static String getConfigFile() {
		
		System.out.println("Specify the path to a JSON file with specifications for this operation:");
		//Enter path to JSON file from standard input (console)
        BufferedReader bufReader = new BufferedReader(new InputStreamReader(System.in)); 
        String configFile = null;
		try {
			configFile = bufReader.readLine();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		return configFile;
	}
	
	/**
	 * Main entry point to the SimSearch application.
	 * @param args  Optional arguments: specify --service is a RESTful service will be deployed.
	 */
	public static void main(String[] args) {

		if (args.length > 0 && args[0].equals("--service")) {  	// SERVICE mode
			try {
				new SimSearchServiceLauncher(args);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		else {            										// STANDALONE mode
			// Instantiate a coordinator for handling all operations
			Coordinator myCoordinator = new Coordinator();
			Scanner in = new Scanner(System.in); 
			do {
				System.out.print("**********Choose a number corresponding to a functionality:**********\n1: MOUNT SOURCES; 2: DELETE SOURCES; 3: CATALOG; 4: SEARCH. Your choice: ");		
				int choice = in.nextInt();
	
				switch (choice) {
				case 1:	 // MOUNTING & INDEXING
					try {
						// Mount the specified data sources to make them available for similarity search; also create indices for ingested data sources
						myCoordinator.mount(getConfigFile());			
					}
					catch (Exception e) {
						e.printStackTrace();
						System.out.println("Mounting stage terminated abnormally.");
					}
					break;
				case 2:  // DATA SOURCE REMOVAL
					try {			        
				        // Invoke removal of the specified data sources
				        myCoordinator.delete(getConfigFile());
					} catch (Exception e) {
						e.printStackTrace();
						System.out.println("Dataset removal terminated abnormally. Make sure that the JSON file provides suitable specifications.");
					}
					break;
				case 3:  // CATALOG DATA SOURCES 
					try {
						// List of data sources available for similarity search queries					
						ObjectWriter ow3 = new ObjectMapper().writer().withDefaultPrettyPrinter();
						String json3 = ow3.writeValueAsString(myCoordinator.listDataSources());
						System.out.println(json3);					
					} catch (Exception e) {
						e.printStackTrace();
						System.out.println("Listing of available data sources terminated abnormally.");
					}
					break;
				case 4:  // SEARCH 
					try {					
				        // Invoke similarity search with the parameters specified in the given JSON configuration
				        SearchResponse[] response = myCoordinator.search(getConfigFile());
				        // Print response as JSON to standard output
						try {
							ObjectWriter ow4 = new ObjectMapper().writer().withDefaultPrettyPrinter();
							String json4 = ow4.writeValueAsString(response);
							System.out.println(json4);
						} catch (JsonProcessingException e) {
							e.printStackTrace();
						}
					} catch (Exception e) {
						e.printStackTrace();
						System.out.println("Query evaluation terminated abnormally. Make sure that the JSON file provides suitable search specifications.");
					}
					break;
				default:   // EXIT (on any other choice)
					System.out.println("Exiting similarity search. All in-memory data will be purged.");
					in.close();
					System.exit(0);
				}	
							
			} while(true);   // loop indefinitely until the user suspends execution	
		}
	}

}
