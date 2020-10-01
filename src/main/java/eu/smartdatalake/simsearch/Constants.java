package eu.smartdatalake.simsearch;

/**
 * Auxiliary class that defines basic default parameters.
 */
public class Constants {

	public static final String LINE_SEPARATOR = "\n"; 
	
	public final static String TOKEN_DELIMITER = ",";
	
	public final static String COLUMN_SEPARATOR = ";";
	
	public final static int KEY_COLUMN = 0;         // By default, the 1st column in the input CSV specifies the keys
	public final static int SEARCH_COLUMN = 1;		// By default, the 2nd column in the input CSV specifies the values to search against
	
	public static final String OUTPUT_FORMAT = "JSON";
	
	public final static String OUTPUT_COLUMN_SEPARATOR = ";";   // Only in case of CSV output files
	public final static String OUTPUT_QUOTE = "\"";   			// Only in case of CSV output files
	
	//FIXME: Specific types of distance measures apply to each facet of similarity search
	public final static int CATEGORICAL_TOPK = 0;   // categorical (set-valued) top-k similarity search
	public final static int SPATIAL_KNN = 1;   		// k-NN spatial similarity search
	public final static int NUMERICAL_TOPK = 2;  	// numerical top-k similarity search
	
	public final static double DECAY_FACTOR = 0.01;      // Default exponential decay constant lambda
	
	public final static int INFLATION_FACTOR = 1000;     // Multiply the top-k with this value to specify the number of partial results to made available from each facet
	
	public final static String RANKING_METHOD = "threshold";	// By default, apply the threshold algorithm in rank aggregation

	public static final String INCORRECT_DBMS = "Incorrect or no value set for the DBMS where input data is stored. Please specify a correct value in the configuration settings.";
	  
	public static final long RANKING_MAX_TIME = 60000;   // Time slot (in milliseconds) dedicated to ranking; otherwise, time out this process and issue all available results

}
