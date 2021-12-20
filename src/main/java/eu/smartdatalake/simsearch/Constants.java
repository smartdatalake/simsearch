package eu.smartdatalake.simsearch;

import java.util.Arrays;
import java.util.List;

/**
 * Auxiliary class that defines basic parameters and their default values.
 */
public class Constants {

	public static final String LINE_SEPARATOR = "\n"; 
	
	public final static String TOKEN_DELIMITER = ",";   // Default delimiter between keywords (tokens)
	
	public final static String WORD_DELIMITER = "+";	// Default delimiter between words in a composite keyword
	
	public final static String COLUMN_SEPARATOR = ";";	// Default separator between columns in a CSV file
	
	public final static int KEY_COLUMN = 0;         // By default, the 1st column in the input CSV specifies the keys
	public final static int SEARCH_COLUMN = 1;		// By default, the 2nd column in the input CSV specifies the values to search against
	
	public static final String OUTPUT_FORMAT = "JSON";
	
	public final static String OUTPUT_COLUMN_SEPARATOR = ";";   // Only in case of CSV output files
	public final static String OUTPUT_QUOTE = "\"";   			// Only in case of CSV output files
	
	//FIXME: Specific types of distance measures apply to each attribute involved in similarity search
	public final static int CATEGORICAL_TOPK = 0;   // categorical (set-valued) top-k similarity search
	public final static int SPATIAL_KNN = 1;   		// k-NN spatial similarity search
	public final static int NUMERICAL_TOPK = 2;  	// numerical top-k similarity search
	public final static int PIVOT_BASED = 3;  		// pivot-based, multi-metric top-k similarity search
	public final static int NAME_DICTIONARY = 4;  	// dictionary of names, not queryable in similarity search
	public final static int KEYWORD_DICTIONARY = 5; // dictionary of keywords, not queryable in similarity search
	public final static int VECTOR_DICTIONARY = 6; 	// dictionary of arrays of values, to be used in PIVOT-based similarity search
	public final static int TEMPORAL_TOPK = 7;  	// temporal top-k similarity search
	public final static int TEXTUAL_TOPK = 8;  		// textual (string) top-k similarity search
	
	public final static double DECAY_FACTOR = 0.05;      // Default exponential decay constant lambda
	
	public final static int QGRAM = 3;		// Default value for q-grams (i.e., trigrams)
	
	public final static int K_MAX = 50;		// Maximum allowable value for top-k most similar results to return per query
	
	public final static int INFLATION_FACTOR = 1000;     // Multiply the top-k with this value to specify the number of partial results to made available from each facet
	
	public final static List<String> RANKING_METHODS = Arrays.asList("threshold", "partial_random_access", "no_random_access", "pivot_based");
	public final static String DEFAULT_METHOD = "threshold";	// By default, apply the threshold algorithm in rank aggregation

	public static final String INCORRECT_DBMS = "Incorrect or no value set for the DBMS where input data is stored. Please specify a correct value in the configuration settings.";
	  
	public static final long RANKING_MAX_TIME = 10000;	// Time (in milliseconds) dedicated to ranking; otherwise, time out this process and issue all available results

	public static final int NUM_PIVOTS = 8;				// Total number of pivot values --> dimensionality of the RR*-tree ; This must be admin-specified
	
	public static final int NODE_FANOUT = 28; 			// Max number of children per node in the RR*-tree
	
	public static final int NUM_SAMPLES = 500;  	// Number of sample points used for estimating pruning potential per metric to be used in RR*-tree construction
	
	// Pattern for SQL-like SELECT queries
	public static final String SQL_SELECT_PATTERN = "SELECT * \r\n" + 
			"    [ FROM running_instance ]\r\n" + 
			"      WHERE attr_name1 ~= 'attr_value1' [ AND ...] \r\n" + 
			"    [ WEIGHTS weight_value1 [, ...] ]\r\n" + 
			"    [ ALGORITHM { threshold | partial_random_access | no_random_access | pivot_based } ]\r\n" + 
			"    [ LIMIT count ]";
	
}
