package eu.smartdatalake.simsearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * Main class for invoking execution of a multi-attribute similarity search request that issues ranked top-k results with aggregate scores.
 * Before issuing any queries, provide a JSON config file that contains specifications for dataset specification for various attributes available for search.
 * Query configurations (contained in JSON files) can be given from standard input once data indexing is done. 
 * EXECUTION command: 
 * java -jar target/simsearch-0.0.1-SNAPSHOT-jar-with-dependencies.jar
 */
public class Runner {	
	
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
	
	public static void main(String[] args) {

		Coordinator myCoordinator = new Coordinator();
/*		
		Runtime runtime = Runtime.getRuntime();
		long usedMemoryBefore = runtime.totalMemory() - runtime.freeMemory();
		System.out.println("Initial memory footprint:" + (1.0 * usedMemoryBefore)/(1024*1024) + "MB");
*/	
		Scanner in = new Scanner(System.in); 
		do {
			System.out.print("**********Choose a number corresponding to a functionality:**********\n1: MOUNT SOURCES; 2: DELETE SOURCES; 3: CATALOG; 4: SEARCH. Your choice: ");		
			int choice = in.nextInt();

			switch (choice) {
			case 1:	 // MOUNTING & INDEXING
				try {
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
					// List of data sources specified					
					ObjectWriter ow3 = new ObjectMapper().writer().withDefaultPrettyPrinter();
					String json3 = ow3.writeValueAsString(myCoordinator.listDataSources());
					System.out.println(json3);
/*					
					AttributeInfo[] response = myCoordinator.listDataSources(getConfigFile());
					ObjectWriter ow3 = new ObjectMapper().writer().withDefaultPrettyPrinter();
					String json3 = ow3.writeValueAsString(response);
					System.out.println(json3);
*/					
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("Listing of available data sources terminated abnormally.");
				}
				break;
			case 4:  // SEARCH 
				try {					
			        // Invoke search with the parameters specified in the JSON configuration
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
//					System.exit(1);
				}
				break;
			default:   // EXIT (on any other choice)
				System.out.println("Exiting similarity search.");
				in.close();
				System.exit(0);
			}	
						
		} while(true);   // loop indefinitely until the user suspends execution	
		
/*
		runtime.gc();
		long usedMemoryAfter = runtime.totalMemory() - runtime.freeMemory();
		System.out.println("Memory footprint: " + (1.0 * (usedMemoryAfter - usedMemoryBefore))/(1024*1024) + "MB");
*/

	}

}
