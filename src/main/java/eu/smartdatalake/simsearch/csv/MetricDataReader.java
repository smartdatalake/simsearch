package eu.smartdatalake.simsearch.csv;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Point;

/**
 * Consumes data from a CSV file and extracts values from a specific attribute.
 * Multi-dimensional point representations are returned, associated with their original object identifiers.
 * Resulting points can be used for generating reference (pivot) points and indexing in multi-distance RR*-tree.
 */
public class MetricDataReader {

    /**
     * Constructor
     */
    public MetricDataReader() {
    }
 
	/**
	 * Creates a dictionary of (key,value) pairs of all items read from a CSV file.
	 * @param inputFile  Path to the input CSV file or its URL at a remote server containing the attribute data.
	 * @param colKey  An integer representing the ordinal number of the attribute containing the key of the entities.
	 * @param colValue  An integer representing the ordinal number of the attribute containing the values of the entities.
	 * @param columnSeparator  The separator character used between attribute values in the CSV file.
	 * @param tokenDelimiter  Delimiter character between tokens in an attribute value.
	 * @param maxLines  Instructs reader to only consume the first lines up to this limit.
	 * @param header  Boolean indicating whether the first line contains attribute names.
	 * @param log  Handle to the logger for statistics and issues over the input data.
	 * @return  A hash map of (key,geometry) values.
	 */
	public TreeMap<String, Point> ingestFromCSVFile(String inputFile, int colKey, int colValue, String columnSeparator, String tokenDelimiter, int maxLines, boolean header, Logger log) {

		TreeMap<String, Point> dict = new TreeMap<String, Point>();
		
		// FIXME: Special handling when delimiter appears in an attribute value enclosed in quotes
        String otherThanQuote = " [^\"] ";
        String quotedString = String.format(" \" %s* \" ", otherThanQuote);
        String regex = String.format("(?x) "+ columnSeparator + "(?=(?:%s*%s)*%s*$)", otherThanQuote, quotedString, otherThanQuote);
        
		int count = 0;
		int errorLines = 0;
		try {
			// Custom reader to handle either local or remote CSV files
			DataFileReader br = new DataFileReader(inputFile);
			String line;
			String[] columns;
	        Point p;

			// If the file has a header, ignore it
			if (header) {
				line = br.readLine();
			}

			// Consume rows and populate the index
			while ((line = br.readLine()) != null) {
				if (maxLines > 0 && count >= maxLines) {
					break;
				}
				try {  // FIXME: Special handling when delimiter appears in an attribute value enclosed in quotes
					columns = line.split(regex,-1);
					if ((columns[colKey].isEmpty()) || (columns[colValue].isEmpty()))
						throw new NullPointerException();
					
					// Generate a point object from the attribute value
                 	String[] tokens = columns[colValue].split(tokenDelimiter);
                 	if (tokens.length > 0) {
 	                	p = Point.create(Arrays.stream(tokens).mapToDouble(Double::parseDouble).toArray());
 						dict.put(columns[colKey], p);
 						count++;
                 	}
                 	else               	
                 		errorLines++;  // NULL (empty) values ignored

				} catch (Exception e) {
					errorLines++;
				}
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (IOException e) {
			e.printStackTrace();
		}

		log.writeln("Finished reading data from file:" + inputFile + ". Items read: " + count
				+ ". Lines skipped due to errors or NULL values: " + errorLines + ".");

		return dict;
	}

	 
	/**
	 * Creates a dictionary of (key,value) pairs of all items read from a CSV file.
	 * This variant concerns properties with values coming from multiple attributes (e.g., locations from lon, lat values held is separate values).
	 * @param inputFile  Path to the input CSV file or its URL at a remote server containing the attribute data.
	 * @param maxLines  Instructs reader to only consume the first lines up to this limit.
	 * @param colKey  An integer representing the ordinal number of the attribute containing the key of the entities.
	 * @param colValues  An array of integers representing the ordinal numbers of the attributes containing the values of the entities.
	 * @param columnSeparator  The separator character used between attribute values in the CSV file.
	 * @param tokenDelimiter  Delimiter character between tokens in an attribute value.
	 * @param header  Boolean indicating whether the first line contains attribute names.
	 * @param log  Handle to the logger for statistics and issues over the input data.
	 * @return  A hash map of (key,geometry) values.
	 */	
	public TreeMap<String, Point> ingestFromCSVFile(String inputFile, int colKey, Integer[] colValues, String columnSeparator, String tokenDelimiter, int maxLines, boolean header, Logger log) {

		TreeMap<String, Point> dict = new TreeMap<String, Point>();
		
		// FIXME: Special handling when delimiter appears in an attribute value enclosed in quotes
        String otherThanQuote = " [^\"] ";
        String quotedString = String.format(" \" %s* \" ", otherThanQuote);
        String regex = String.format("(?x) "+ columnSeparator + "(?=(?:%s*%s)*%s*$)", otherThanQuote, quotedString, otherThanQuote);
        
		int count = 0;
		int errorLines = 0;
		try {
			// Custom reader to handle either local or remote CSV files
			DataFileReader br = new DataFileReader(inputFile);
			String line;
			String[] columns;
	        Point p;

			// If the file has a header, ignore it
			if (header) {
				line = br.readLine();
			}

			// Consume rows and populate the index
			while ((line = br.readLine()) != null) {
				if (maxLines > 0 && count >= maxLines) {
					break;
				}
				try {  // FIXME: Special handling when delimiter appears in an attribute value enclosed in quotes
					columns = line.split(regex,-1);
					if (columns[colKey].isEmpty())
						throw new NullPointerException();
					
					// Collect all values from the user-specified columns
					List<String> myValues = new ArrayList<String>();
					for (int colValue: colValues) 
						if (columns[colValue].isEmpty())
							throw new NullPointerException();
						else {
							myValues.addAll(Arrays.asList(columns[colValue].split(tokenDelimiter)));
						}
						
					// Generate a point object from the attribute values
                 	String[] tokens = myValues.toArray(new String[0]);
                 	if (tokens.length > 0) {
 	                	p = Point.create(Arrays.stream(tokens).mapToDouble(Double::parseDouble).toArray());
 						dict.put(columns[colKey], p);
 						count++;
                 	}
                 	else               	
                 		errorLines++;  // NULL (empty) values ignored

				} catch (Exception e) {
					errorLines++;
				}
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (IOException e) {
			e.printStackTrace();
		}

		log.writeln("Finished reading data from file:" + inputFile + ". Items read: " + count
				+ ". Lines skipped due to errors or NULL values: " + errorLines + ".");

		return dict;	
	}
	 
}
