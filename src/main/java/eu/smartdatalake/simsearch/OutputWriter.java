package eu.smartdatalake.simsearch;

import java.io.FileNotFoundException;
import java.io.PrintStream;

import org.json.simple.JSONObject;

import eu.smartdatalake.simsearch.ranking.RankedResult;

/**
 * Auxiliary class that can be used to write search results to a file in CSV format.
 */
public class OutputWriter {

	PrintStream outStream = null;
	String outColumnDelimiter = Constants.OUTPUT_COLUMN_SEPARATOR;   // Default delimiter for values in the output CSV file
	boolean outHeader = true;
	
	public OutputWriter(JSONObject config) {
		
		// Output format
		if (config.get("output_format") != null) {
			String outFormat = String.valueOf(config.get("output_format"));
			if (outFormat.toLowerCase().equals("csv")) {
				if (config.get("column_delimiter") != null) {
					outColumnDelimiter = String.valueOf(config.get("column_delimiter"));
					if (outColumnDelimiter == null || outColumnDelimiter.equals(""))
						outColumnDelimiter = " ";
				}
				if (config.get("header") != null)
					outHeader = Boolean.parseBoolean(String.valueOf(config.get("header")));
				
				// output file
				String outputFile = String.valueOf(config.get("output_file"));
				try {
					outStream = new PrintStream(outputFile);    // An output stream is only created in case of CSV format
				} catch (FileNotFoundException e) {
					e.printStackTrace();
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
	 * Closes the output writer.
	 */
	public void close() {
		outStream.close();
	}
	
	/**
	 * Writes the search results into the CSV file.
	 * @param results  Output results as received from the rank aggregation process (for a given combination of weights).
	 */
	public void writeResults(RankedResult[] results) {
		
		if (results.length > 0)
			outStream.println(results[0].getColumnNames(outColumnDelimiter));
		for (RankedResult res: results) {
			outStream.println(res.toString(outColumnDelimiter));
		}
		// TODO: Avoid mixing of results in case of multiple combinations of weights; A new file should be created for each combination
		outStream.println("**************************************************");
	}
	
}
