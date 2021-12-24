package eu.smartdatalake.simsearch.engine;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import eu.smartdatalake.simsearch.Assistant;
import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.request.SearchOutput;

/**
 * Auxiliary class that can be used to write search results to a file in CSV format or to standard output (console).
 */
public class OutputWriter {

	Assistant myAssistant;
	PrintStream outStream = null;
	String outColumnDelimiter = null;
	boolean outHeader = true;
	String outQuote = null;		
	public boolean outJsonFile = false;	// Indicates whether the output will be written to a JSON file
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
					else   // Default delimiter
						outColumnDelimiter = Constants.COLUMN_SEPARATOR;
					
					if (out.header != null)
						outHeader = out.header;
					
					if (out.quote != null) {
						outQuote = out.quote;
						if (outQuote == null || outQuote.equals(""))
							outQuote = Constants.OUTPUT_QUOTE;   	// Default quote for string values in the output CSV file
					}
					
					// output file
					try {
						outStream = new PrintStream(out.file);
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					}
				}
				else { 	
					try {
						if (out.file != null)
							outStream = new PrintStream(out.file);	// An output stream for TXT or JSON files
						if (outFormat.toLowerCase().equals("json"))
							outJsonFile = true;		// Output will be written to a JSON file
						else if (outFormat.toLowerCase().equals("txt"))
							outStandard = true;		// Output in tabular format to standard output (console)
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
	 * Prints the search results into a TXT file or to the standard output (console).
	 * @param weights  The weights applied to the specified batch of results. Assuming only ONE weight value per attribute.
	 * @param results  Output results as received from the rank aggregation process (for a given combination of weights).
	 * @param responseTime  Time (in seconds) taken to provide the response in the backend.
	 */
	public void printResults(Map<String, Double> weights, IResult[] results, double responseTime) {
		
		SearchResponseTable tab = new SearchResponseTable();
		String txtResult = tab.print(weights, results, responseTime);
		if (this.isSet())
			outStream.println(txtResult);   // Write tabular results to TXT file
		else
			System.out.println(txtResult);	// Issue tabular results to console
	}
	
	/**
	 * Writes the response to a search request into a JSON file.
	 * @param response   A JSON response to a SimSearch request.
	 */
	public void writeJsonResults(Object response) {
		
		try {
			ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
			String jsonResult = ow.writeValueAsString(response);
			outStream.println(jsonResult);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}	
	}
	
}
