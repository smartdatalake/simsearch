package eu.smartdatalake.simsearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import eu.smartdatalake.simsearch.engine.SqlParser;
import eu.smartdatalake.simsearch.manager.AttributeInfo;
import eu.smartdatalake.simsearch.service.SimSearchServiceLauncher;

/**
 * Main class for invoking execution of a multi-attribute similarity search request that issues ranked top-k results with aggregate scores.
 * Before issuing any queries, attribute data sources must be mounted (either ingested from CSV files or in-situ from JDBC connections or REST APIs) using a JSON configuration.
 * Query configurations (specified in JSON) can be submitted once data sources have been mounted. 
 * 
 * Execution command -- STANDALONE mode with requests specified from standard input: 
 * java -jar target/simsearch-0.5-SNAPSHOT.jar
 * 
 * Execution command -- SERVICE mode: for launching a web service, e.g., at port 8090, and specifying requests using the REST API: 
 * java -Dserver.port=8090 -jar target/simsearch-0.5-SNAPSHOT.jar --service
 */
public class Runner {	
	
	public static final String ANSI_RESET = "\u001B[0m";
	public static final String ANSI_BLACK = "\u001B[30m";
	public static final String ANSI_BLUE = "\u001B[34m";
	public static final String BLUE_BRIGHT = "\033[0;94m";
	public static final String GREEN_BRIGHT = "\033[0;92m";
	public static final String RED_BRIGHT = "\033[0;91m";
	
	/**
	 * Provides the path to the JSON file containing configurations for the various requests.
	 * @return  Path to a JSON file.
	 */
	private static String getConfigFile() {
		
		//Enter path to JSON file from standard input (console)
		System.out.println("Specify the path to a JSON file with specifications for this operation:");
		// Accept user input from standard input
		BufferedReader bufReader = new BufferedReader(new InputStreamReader(System.in)); 
        String in = null;
		try {
			in = bufReader.readLine();		
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		return in;
	}
	

	/**
	 * Read multiple lines from standard input (console). 
	 * @param in  Scanner of the standard input.
	 * @return  The read lines as a unified string with a blank character instead of line breaks.
	 */
    public static String getMultiLineCommand(Scanner in) {
    	
    	String lines = "";
    	int i = 0;
        while (in.hasNextLine()) {
            String line = in.nextLine();
            if (line != null) {
            	// Detect a comment within SQL statements and ignore it
            	if ((i = line.indexOf("--")) > 0)   
            		line = line.substring(0, i);
            	lines += " " + line;
            	// Detect specific symbols
            	if (line.contains(";") || (line.startsWith("\\d")) || (line.startsWith("\\q")))
                    break;
            }
        }
        
        return lines.trim();       
    }
    
    
	/**
	 * Prints a JSON response to standard output.
	 * @param val  A JSON response to a SimSearch request.
	 * @param msg  A message notification to display in case of errors.
	 */
	private static void printResponse(Object val, String msg) {
		
		try {
			ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
			String json = ow.writeValueAsString(val);
			System.out.println(json);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			System.out.println(msg);
		}
	}
	
	
	/**
	 * Main entry point to the SimSearch application.
	 * @param args  Optional arguments: specify --service is a RESTful service will be deployed; otherwise, a standalone instance will be deployed.
	 */
	public static void main(String[] args) {

		if (args.length > 0 && args[0].equals("--service")) {  	// SERVICE mode
			try {
				new SimSearchServiceLauncher(args);   // Arguments may include a JSON file with predefined API key to be specifically used with the service
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		else {            										// STANDALONE mode
			// Instantiate a coordinator for handling all operations
			Coordinator myCoordinator = new Coordinator();
			Scanner in = new Scanner(System.in); 
			do {
				System.out.print("**********Choose a number corresponding to a functionality:**********\n1: MOUNT SOURCES; 2: DELETE SOURCES; 3: CATALOG; 4: SEARCH; 5: SQL TERMINAL. Your choice: ");		
				int choice = in.nextInt();
	
				switch (choice) {
				case 1:	 // MOUNTING & INDEXING
					// Mount the specified data sources to make them available for similarity search; also create indices for ingested data sources
					printResponse(myCoordinator.mount(getConfigFile()), "Mounting stage terminated abnormally.");
					break;
				case 2:  // DATA SOURCE REMOVAL
					// Invoke removal of the specified data sources
					printResponse(myCoordinator.delete(getConfigFile()), "Dataset removal terminated abnormally. Make sure that the JSON file provides suitable specifications.");
					break;
				case 3:  // CATALOG DATA SOURCES 
					// List all data sources available for similarity search queries	
					printResponse(myCoordinator.listDataSources(), "Listing of available data sources terminated abnormally.");
					break;
				case 4:  // SEARCH 
					 // Invoke similarity search with the parameters specified in the given JSON configuration
					printResponse(myCoordinator.search(getConfigFile()), "Query evaluation terminated abnormally. Make sure that the JSON file provides suitable search specifications.");
					break;
				case 5:	 // SQL TERMINAL
					System.out.println("Entering SQL terminal for SELECT queries. \nType " + RED_BRIGHT + "\\d" + ANSI_RESET + " to list queryable attributes. \nType " + RED_BRIGHT + "\\q" + ANSI_RESET + " to exit.");	
					boolean sqlMode = true;
					do {
						System.out.print(GREEN_BRIGHT + "SQL >" + ANSI_RESET + " ");
						String q = getMultiLineCommand(in);
						if (q.startsWith("\\q")) {  // Exit SQL terminal
							sqlMode = false;
						}
						else if (q.startsWith("\\d")) {  // Describe (list) data sources
							AttributeInfo[] info = myCoordinator.listDataSources();
							System.out.println();
							System.out.printf(" %-25s  %-25s  %-25s  %-10s\n", "Attribute", "Type", "Operation", "Ingested");
							System.out.println(" -------------------------  -------------------------  -------------------------  --------- ");
							for (AttributeInfo a: info) {
								System.out.printf(" %-25s  %-25s  %-25s  %-10s\n", a.getColumn(), a.getDatatype(), a.getOperation(), ""+a.isIngested());
							}
							System.out.println();
						}
						else {	// Parse the submitted SQL query
							if (q.toLowerCase().startsWith("select")) {
								SqlParser sqlparser = new SqlParser();
								myCoordinator.search(sqlparser.parseSelect(q));
							}
							else if (q.toLowerCase().startsWith("set")) {   // Set global setting for this instance
								q = q.toLowerCase().trim();					// e.g.: SET query_timeout 20000;
								String[] tokens = q.substring(0, q.length() - 1).split("\\s+");
								long query_timeout = Long.parseLong(tokens[2]);
								if (tokens[1].equals("query_timeout")) { 
									myCoordinator.instanceSettings.settings.index.setQueryTimeout(query_timeout);
									System.out.println("Query timeout set to " + myCoordinator.instanceSettings.settings.index.getQueryTimeout() + " ms.");
								}
							}
							else
								System.out.println("SQL terminal accepts SELECT statements only.");
						}
					} while (sqlMode);   // loop indefinitely until the user exits SQL terminal	
					System.out.println("SQL terminal closed.");
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
