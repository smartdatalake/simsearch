package eu.smartdatalake.simsearch.engine;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Map;

import eu.smartdatalake.simsearch.Assistant;
import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.request.SearchOutput;

/**
 * Auxiliary class that can be used to write search results to a file in CSV format or to standard output (console).
 */
public class OutputWriter {

	Assistant myAssistant;
	PrintStream outStream = null;
	String outColumnDelimiter = Constants.OUTPUT_COLUMN_SEPARATOR;	// Default delimiter for values in the output CSV file
	boolean outHeader = true;
	String outQuote = null;		
	boolean outStandard = false;	// Indicates whether output will be written to standard output (console)

	/**
	 * Constructor
	 * @param out   Output specifications for search results.
	 */
	public OutputWriter(SearchOutput out) {
		
		myAssistant = new Assistant();
		
		if (out!= null) {    // Output specifications may be missing in case of JSON (default)
			if (out.format != null) {
				String outFormat = out.format;
				if (outFormat.toLowerCase().equals("csv")) {   // Output will be stored in a CSV file
					if (out.delimiter != null) {
						outColumnDelimiter = out.delimiter;
						if (outColumnDelimiter == null || outColumnDelimiter.equals(""))
							outColumnDelimiter = " ";
					}
					if (out.header != null)
						outHeader = out.header;
					
					if (out.quote != null) {
						outQuote = out.quote;
						if (outQuote == null || outQuote.equals(""))
							outQuote = Constants.OUTPUT_QUOTE;   	// Default quote for string values in the output CSV file
					}
					
					// output file
					String outputFile = out.file;
					try {
						outStream = new PrintStream(outputFile);    // An output stream is only created in case of CSV format
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					}
				}
				else if (outFormat.toLowerCase().equals("console")) { // Output written to console
					try {
						outStandard = true;
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	/**
	 * Indicates whether an output stream has been specified for writing search results in CSV format
	 * @return  True, if the output stream has been defined; otherwise, False.
	 */
	public boolean isSet() {
		
		return (outStream != null);
	}
	
	/**
	 * Indicates whether output will be written to standard output (console).
	 * @return  True, if the output will be printed to the console; otherwise, False.
	 */
	public boolean isStandardOutput() {
		
		return outStandard;
	}
	
	/**
	 * Closes the output writer.
	 */
	public void close() {
		outStream.close();
	}
	
	
	/**
	 * Writes the search results into the CSV file.
	 * @param weights  The weights applied to the specified batch of results. These weights will become the first column in the output CSV file.
	 * @param results  Output results as received from the rank aggregation process (for a given combination of weights).
	 */
	public void writeResults(Double[] weights, IResult[] results) {
		
		// Print header only once, for the results corresponding to the first combination of weights
		if (outHeader) {
			outStream.println("weights" + outColumnDelimiter + results[0].getColumnNames(outColumnDelimiter));
			outHeader = false;    
		}
		
		// To distinguish results in case of multiple combinations of weights, the first column denotes the applied weights
		for (IResult res: results) {
			if (outQuote != null)
				outStream.println((Arrays.toString(weights).contains(outColumnDelimiter) ? outQuote + Arrays.toString(weights) + outQuote : Arrays.toString(weights)) + outColumnDelimiter + res.toString(outColumnDelimiter, outQuote));
			else
				outStream.println(Arrays.toString(weights) + outColumnDelimiter + res.toString(outColumnDelimiter));
		}
	}
	
	/**
	 * Prints the search results into the standard output (console).
	 * @param weights  The weights applied to the specified batch of results. Assuming only ONE weight value per attribute.
	 * @param results  Output results as received from the rank aggregation process (for a given combination of weights).
	 * @param responseTime  Time (in seconds) taken to provide the response in the backend.
	 */
	public void printResults(Map<String, Double> weights, IResult[] results, double responseTime) {
		
		SearchResponseTable tab = new SearchResponseTable();
		tab.print(weights, results, responseTime);
	}
	
}
