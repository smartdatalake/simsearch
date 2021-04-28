package eu.smartdatalake.simsearch.manager.ingested.temporal;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.manager.ingested.DataFileReader;
import eu.smartdatalake.simsearch.manager.ingested.numerical.BPlusTree;
import eu.smartdatalake.simsearch.manager.insitu.JdbcConnector;

/**
 * Consumes data from a CSV file or a table over a JDBC connection and extracts date/time values from a specific attribute.
 * Values are maintained in a hash table taking their keys from another attribute in the file or table. 
 * A B+-tree index may be created from the collected (key,value) pairs.
 */
public class DateTimeReader {

	public int count;
	public Map<Integer, String> columnNames = null;
	DateTimeParser parser;

	/**
	 * Default constructor
	 */
	public DateTimeReader() {
		
		parser = new DateTimeParser();
	}
	
	/**
	 * Constructor employing a user-specified format for date/time values.
	 * @param format  Format of date/time values.
	 */
	public DateTimeReader(String format) {
		
		parser = new DateTimeParser(format);
	}
	
	/**
	 * Creates a dictionary of (key,value) pairs of all items read from a CSV file.
	 * ASSUMPTION: Input data collection consists of (key, value) pairs of strings: an identifier and a date/time string
	 * IMPORTANT! The date/time value is converted into a double number in order to be indexed in a B+-tree.
	 * @param inputFile  Path to the input CSV file or its URL at a remote server containing the attribute data.
	 * @param maxLines  Number of the first lines to read from the file, skipping the rest; if a negative value is specified, all lines will be consumed.
	 * @param colKey  Ordinal number of the attribute holding the entity identifiers.
	 * @param colValue  Ordinal number of the column containing a given temporal attribute of these entities.
	 * @param columnDelimiter  Delimiter character between columns in the file.
	 * @param header  Boolean indicating that the first line contains the names of the attributes.
	 * @param log  Handle to the log file for keeping messages and statistics.
	 * @return  A dictionary (i.e., a hash map) of the (key,value) pairs.
	 */
	public HashMap<String, Double> importFromCsvFile(String inputFile, int maxLines, int colKey, int colValue, String columnDelimiter, boolean header, Logger log) {

		HashMap<String, Double> dict = new HashMap<String, Double>();

		// FIXME: Special handling when delimiter appears in an attribute value enclosed in quotes
        String otherThanQuote = " [^\"] ";
        String quotedString = String.format(" \" %s* \" ", otherThanQuote);
        String regex = String.format("(?x) "+ columnDelimiter + "(?=(?:%s*%s)*%s*$)", otherThanQuote, quotedString, otherThanQuote);
        
		int errorLines = 0;
		try {
			// Custom reader to handle either local or remote CSV files
			DataFileReader br = new DataFileReader(inputFile);
			String line;
			String[] columns;
			double v;

			// if the file has a header, retain the names of the columns for possible future use
			if (header) {
				line = br.readLine();
				columns = line.split(regex,-1);				
				columnNames = new HashMap<Integer, String>();
				for (int i = 0; i < columns.length; i++) {
					columnNames.put(i, columns[i]);
				}
			}
			// Consume rows and populate the index
			while ((line = br.readLine()) != null) {
				if (maxLines > 0 && count >= maxLines) {
					break;
				}
				try {
					columns = line.split(regex,-1);
					if ((columns[colKey].isEmpty()) || (columns[colValue].isEmpty())) 
						throw new NullPointerException();
					v = parser.parseDateTimeToEpoch(columns[colKey]);
					dict.put(columns[colValue], v);
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

		log.writeln("Finished reading data on " + columnNames.get(colKey) + " from file: " + inputFile + ". Items read: " + count
				+ ". Lines skipped due to errors: " + errorLines + ".");

		return dict;
	}


	/**
	 * Reading date/time attribute data from a table in a DBMS over a JDBC connection.
	 * @param tableName  Name of the table that holds the attribute data.
	 * @param keyColumnName  Name of the attribute holding the entity identifiers (keys)
	 * @param valColumnName  Name of the attribute containing date/time values of these entities.
	 * @param jdbcConnector  The JDBC connection that provides access to the table.
	 * @param log  Handle to the log file for keeping messages and statistics.
	 * @return  A dictionary (i.e., a hash map) of the (key,value) pairs.
	 */
	public HashMap<String, Double> importFromJdbcTable(String tableName, String keyColumnName, String valColumnName, JdbcConnector jdbcConnector, Logger log) {

		HashMap<String, Double> dict = new HashMap<String, Double>();

		long startTime = System.nanoTime();
		// In case no column for key identifiers has been specified, use the primary key of the table  
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
			 //Execute SQL query in the DBMS and fetch all NOT NULL values available for this attribute
			 String sql = "SELECT " + keyColumnName + ", " + valColumnName + " FROM " + tableName + " WHERE " + valColumnName + " IS NOT NULL";
//			 System.out.println("TEMPORAL query: " + sql);
			 rs = jdbcConnector.executeQuery(sql);  
			 // Iterate through all retrieved results, parse temporal values and put them to the in-memory look-up
		     while (rs.next()) { 
		    	 dict.put(rs.getString(1), parser.parseDateTimeToEpoch(rs.getString(2)));
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
	 * Builds a B+-tree index based on (key,value) pairs available in a collection.
	 * CAUTION! Indexes original features without applying normalization.
	 * @param targetData  The collection of data given as (key, value) pairs.
	 * @param log  Handle to the log file for keeping messages and statistics.
	 * @return  A handle to the root of the created B+-tree.
	 */
	public BPlusTree<Double, String> buildIndex(Map<String, Double> targetData, Logger log) {

		BPlusTree<Double, String> index = new BPlusTree<Double, String>(64);
		
		int lineCount = 0;
		try {
			// Consume data and populate the index
			for (Map.Entry<String, Double> entry : targetData.entrySet()) {
				//CAUTION! Values (doubles) are used as keys for internal nodes in the B+-tree
				index.insert(entry.getValue(), entry.getKey());
				lineCount++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		log.writeln("Finished indexing data. Items indexed: " + lineCount + ".");

		return index;
	}

	
	/**
	 * Get the name of an attribute, as read from the input file header.
	 * @param i  The i-th attribute in the list (starting from 0).
	 * @return  The name of the attribute.
	 */
	public String getColumnName(int i) {
		if (columnNames != null) {
			return columnNames.get(i);
		}
		
		return null;   // No column names are specified
	}
	
}
