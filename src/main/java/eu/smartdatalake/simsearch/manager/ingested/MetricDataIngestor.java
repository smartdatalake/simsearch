package eu.smartdatalake.simsearch.manager.ingested;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.engine.QueryValueParser;
import eu.smartdatalake.simsearch.manager.DataType.Type;
import eu.smartdatalake.simsearch.manager.ingested.temporal.DateTimeParser;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Point;

/**
 * Ingests metric data from a CSV file or a DBMS table and extracts values from a specific attribute.
 * Multi-dimensional point representations are returned, associated with their original object identifiers.
 * Resulting points can be used for generating reference (pivot) points and indexing in multi-distance RR*-tree.
 */
public class MetricDataIngestor {
	
    /**
     * Constructor
     */
    public MetricDataIngestor() {
    	
    }
 
 
	/**
	 * Examines indicative value of an attribute in a CSV file in order to determine its data type.
	 * @param inputFile  Path to the input CSV file or its URL at a remote server containing the attribute data.
	 * @param colValue  An integer representing the ordinal number of the attribute containing the values of the entities.
	 * @param columnSeparator  The separator character used between attribute values in the CSV file.
	 * @param header  Boolean indicating whether the first line contains attribute names.
	 * @return  The data type of the specified attribute.
	 */
	public Type getDataType(String inputFile, int colValue, String columnSeparator, boolean header) {
			
        QueryValueParser valParser = new QueryValueParser();
        Type dtype = Type.UNKNOWN;
        
		// FIXME: Special handling when delimiter appears in an attribute value enclosed in quotes
        String otherThanQuote = " [^\"] ";
        String quotedString = String.format(" \" %s* \" ", otherThanQuote);
        String regex = String.format("(?x) "+ columnSeparator + "(?=(?:%s*%s)*%s*$)", otherThanQuote, quotedString, otherThanQuote);
        
		try {
			// Custom reader to handle either local or remote CSV files
			DataFileReader br = new DataFileReader(inputFile);
			String line;
			String[] columns;

			// If the file has a header, ignore it
			if (header) {
				line = br.readLine();
			}

			// Consume rows
			while ((line = br.readLine()) != null) {
				try {  // FIXME: Special handling when delimiter appears in an attribute value enclosed in quotes
					columns = line.split(regex,-1);
					if (columns[colValue].isEmpty())
						continue;
					
					// Parse the first NOT NULL attribute value
                 	if (!columns[colValue].isEmpty()) {
                 		valParser.parse(columns[colValue]);
                 		dtype = valParser.getDataType();
                 		return dtype;
                 	}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return dtype;
	}
	 
	
	/**
	 * Creates a dictionary of (key,point) pairs of all items read from a CSV file.
	 * Concerns properties with values coming from a single or multiple attributes (e.g., locations from lon, lat values held is separate values).
	 * @param inputFile  Path to the input CSV file or its URL at a remote server containing the attribute data.
	 * @param maxLines  Instructs reader to only consume the first lines up to this limit.
	 * @param colKey  An integer representing the ordinal number of the attribute containing the key of the entities.
	 * @param colValues  An array of integers representing the ordinal numbers of the attributes containing the values of the entities.
	 * @param columnSeparator  The separator character used between attribute values in the CSV file.
	 * @param tokenDelimiter  Delimiter character between tokens in an attribute value.
	 * @param header  Boolean indicating whether the first line contains attribute names.
	 * @param log  Handle to the logger for statistics and issues over the input data.
	 * @return  A hash map of (key,point) values.
	 */	
	public TreeMap<String, Point> importFromCsvFile(String inputFile, int colKey, Integer[] colValues, String columnSeparator, String tokenDelimiter, int maxLines, boolean header, Logger log) {

		TreeMap<String, Point> dict = new TreeMap<String, Point>();
		
		// FIXME: Special handling when delimiter appears in an attribute value enclosed in quotes
        String otherThanQuote = " [^\"] ";
        String quotedString = String.format(" \" %s* \" ", otherThanQuote);
        String regex = String.format("(?x) "+ columnSeparator + "(?=(?:%s*%s)*%s*$)", otherThanQuote, quotedString, otherThanQuote);
        
        // Determine if the given attribute contains date/time values
        DateTimeParser dateParser = null;
        if (getDataType(inputFile, colValues[0], columnSeparator, header) == Type.DATE_TIME)
        	dateParser = new DateTimeParser();
        
		int count = 0;
		int errorLines = 0;
		String colValueNames = "";
		try {
			// Custom reader to handle either local or remote CSV files
			DataFileReader br = new DataFileReader(inputFile);
			String line;
			String[] columns;
	        Point p;

			// If the file has a header, ignore it
			if (header) {
				line = br.readLine();
				columns = line.split(regex,-1);
				for (int colValue: colValues)
					colValueNames += columns[colValue] + ", ";   // Detect attribute names
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
                 		if (dateParser != null) // Special handling for date/time values: conversion to epoch
                 			p = Point.create(new double[]{dateParser.parseDateTimeToEpoch(tokens[0])});
                 		else
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

		log.writeln("Finished reading data on " + colValueNames + " from file:" + inputFile + ". Items read: " + count
				+ ". Lines skipped due to errors or NULL values: " + errorLines + ".");

		return dict;	
	}

}
