package eu.smartdatalake.simsearch.manager.ingested.lookup;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Pattern;

import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.manager.ingested.DataFileReader;
import eu.smartdatalake.simsearch.manager.insitu.JdbcConnector;

public class DictionaryReader<K,V> {

	public int count;
	Pattern pattern;
	Class<?> valueType;
	
	/**
	 * Constructor #1
	 */
	public DictionaryReader() {
		
		valueType = null;
		pattern = null;
	}
	
	
	/**
	 * Constructor #2
	 * @param tokDelimiter  Delimiter to be used for separating values into an array in the dictionary
	 * @param valueType   The data type of the values to hold in the dictionary, e.g., Double, Integer.
	 */
	public DictionaryReader(String tokDelimiter, Class<?> valueType) {
		
		this.valueType = valueType;
		
		// Pattern to apply in splitting the value into an array
		if (tokDelimiter != null)
			pattern = Pattern.compile(tokDelimiter);
	}
	
	
	/**
	 * Creates a dictionary of (key,value) pairs of all items read from a table in a DBMS over a JDBC connection.
	 * @param tableName  Name of the table that holds the attribute data.
	 * @param keyColumnName  Name of the attribute holding the entity identifiers (keys)
	 * @param valColumnName  Name of the attribute containing a given value attribute (e.g., name) of these entities.
	 * @param jdbcConnector  The JDBC connection that provides access to the table.
	 * @param log  Handle to the log file for keeping messages and statistics.
	 * @return  A dictionary (i.e., a hash map) of the (key,value) pairs.
	 */
	public HashMap<K, V> importFromJdbcTable(String tableName, String keyColumnName, String valColumnName, JdbcConnector jdbcConnector, Logger log) {

		HashMap<K, V> dict = new HashMap<K, V>();

		long startTime = System.nanoTime();
		// In case no column for key datasetIdentifiers has been specified, use the primary key of the table  
		if (keyColumnName == null) {
			// Assuming that primary key is a single attribute (column), this query can retrieve it 
			// FIXME: Currently working with PostgreSQL only
			keyColumnName = jdbcConnector.getPrimaryKeyColumn(tableName);
			if (keyColumnName == null)  // TODO: Handle other JDBC sources
				return null;
		}

	  	ResultSet rs;	
	  	int n = 0;
		try { 
//			 System.out.println("QUERY: SELECT " + keyColumnName + ", " + valColumnName + " FROM " + tableName + " WHERE " + valColumnName + " IS NOT NULL;");
			 //Execute SQL query in the DBMS and fetch all NOT NULL values available for this attribute
			 String sql = "SELECT " + keyColumnName + ", " + valColumnName + " FROM " + tableName + " WHERE " + valColumnName + " IS NOT NULL";
//			 System.out.println("STRING VALUE query: " + sql);
			 rs = jdbcConnector.executeQuery(sql);  
			 // Iterate through all retrieved results and put them to the in-memory look-up
		     while (rs.next()) { 
		    	 // Cast items to the corresponding data type
		    	 dict.put((K)rs.getString(1), (pattern != null ? castValue(pattern.split(rs.getString(2))) : (V) rs.getString(2)));
		    	 n++;
		      }
		     log.writeln("Extracted " + n + " data values on " + valColumnName + " from database table " + tableName + " in " + (System.nanoTime() - startTime) / 1000000000.0 + " sec.");
		 }
		 catch(Exception e) { 
				log.writeln("An error occurred while retrieving data from the database.");
				e.printStackTrace();
		 }
		 		 
		 return dict;
	}
	
	
	/**
	 * Creates a dictionary of (key,value) pairs of all items read from a CSV file.
	 * ASSUMPTION: Input data collection consists of pairs of identifiers (KEY) and elements (VALUE)
	 * @param inputFile  Path to the input CSV file or its URL at a remote server containing the attribute data.
	 * @param maxLines  Number of the first lines to read from the file, skipping the rest; if a negative value is specified, all lines will be consumed.
	 * @param colKey  Ordinal number of the attribute holding the entity identifiers.
	 * @param colValue  Ordinal number of the column containing a given value attribute (e.g., name) of these entities.
	 * @param columnDelimiter  Delimiter character between columns in the file.
	 * @param header  Boolean indicating that the first line contains the names of the attributes.
	 * @param log  Handle to the log file for keeping messages and statistics.
	 * @return  A dictionary (i.e., a hash map) of the (key,value) pairs.
	 */
	public HashMap<K, V> importFromCsvFile(String inputFile, int maxLines, int colKey, int colValue, String columnDelimiter, boolean header, Logger log) {

		HashMap<K, V> dict = new HashMap<K, V>();
		count = 0;

		// FIXME: Special handling when delimiter appears in an attribute value enclosed in quotes
        String otherThanQuote = " [^\"] ";
        String quotedString = String.format(" \" %s* \" ", otherThanQuote);
        String regex = String.format("(?x) "+ columnDelimiter + "(?=(?:%s*%s)*%s*$)", otherThanQuote, quotedString, otherThanQuote);
        
		int errorLines = 0;
		boolean flagArray = false;
		String colValueName = "dictionary";   // Just in case no column name is available in the file
		try {
			// Custom reader to handle either local or remote CSV files
			DataFileReader br = new DataFileReader(inputFile);
			String line;
			String[] columns;

			// If no column is specified for values, then all columns except the one used as key will be held as an array of values
			if (colValue < 0) {
				pattern = null;
				flagArray = true;  // All values except the first will be held in an array
			}
				
			// If the file has a header, skip it
			if (header) {
				line = br.readLine();
				columns = line.split(regex,-1);
				colValueName = columns[colValue];   // Detect attribute name
			}

			// Consume rows and populate the index
			while ((line = br.readLine()) != null) {
				if (maxLines > 0 && count >= maxLines) {
					break;
				}
				try {
					columns = line.split(regex,-1);
					if ((columns[colKey].isEmpty()) || ((colValue >= 0) && (columns[colValue].isEmpty()))) 
						throw new NullPointerException();
					// Cast items to the corresponding data type
					dict.put((K)columns[colKey], (pattern != null ? castValue(pattern.split(columns[colValue])) : (flagArray ? castValue(Arrays.copyOfRange(columns, colKey+1, columns.length)) : (V)columns[colValue])));
					count++;
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

		log.writeln("Finished reading data on " + colValueName + " from file: " + inputFile + ". Items read: " + count
				+ ". Lines skipped due to errors: " + errorLines + ".");

		return dict;
	}
	
	/**
	 * Casts an array of string values into an array of values in the requested data type.
	 * @param arr  The input array of string values.
	 * @return  The cast array of values in the desired data type.
	 */
	private V castValue(String[] arr) {
		
		if (valueType == Double.class) { 
			// Convert to array of double values
			return (V) Arrays.stream(arr)
                .mapToDouble(Double::parseDouble)
                .toArray();
		}
		else if (valueType == Integer.class) {
			// Convert to array of integers
			return (V) Arrays.stream(arr)
	                .mapToInt(Integer::parseInt)
	                .toArray();
		}
		// TODO: Handle other possible data types
		
		return (V)arr;   // Return the original array of string values
	}
	
}
