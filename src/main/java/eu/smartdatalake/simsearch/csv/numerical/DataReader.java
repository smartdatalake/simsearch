package eu.smartdatalake.simsearch.csv.numerical;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.jdbc.JdbcConnector;

/**
 * Consumes data from a CSV file or over a JDBC connection and extracts numerical values from a specific attribute.
 * Values are maintained in a hash table taking their keys from another attribute in the file. 
 * Aggregate statistics are also collected while parsing the file.
 * A B+-tree index may be created from the collected (key,value) pairs.
 */
public class DataReader {

	// Basic statistics about the numeric values read in the input collection
	public double minVal, maxVal, avgVal, stDev;
	public int count;
	public Map<Integer, String> columnNames = null;


	/**
	 * Distributive statistics like MIN or MAX can be calculated progressively, as input values are consumed.
	 * @param x  Current input value to be used in adjusting statistics.
	 */
	private void adjustDistributiveStats(double x) {
		
		if (x > maxVal)
			maxVal = x;
		if (x < minVal)
			minVal = x;	
	}
	 
	/**
	 * Algebraic statistics like AVG or STDEV require the results from previously executed distributive statistics
	 * @param values  The collection of input (numerical) values.
	 */
	private void calculateAlgebraicStats(Collection<Double> values) {
		
		avgVal = (maxVal - minVal) / count;
		
		double sqDiff = 0.0;
		double v;
		for (Iterator<Double> iter = values.iterator(); iter.hasNext();) {
			v = iter.next();
			sqDiff += (v - avgVal)* (v - avgVal);
		}

		stDev = Math.sqrt(sqDiff / (count - 1));
	}
	
	/** NOT CURRENTLY USED!
	 * Builds a B+-tree index according to user's specifications
	 * ASSUMPTION: Input data collection consists of pairs of doubles (KEY) and strings (VALUE)
	 * @param inputFile  
	 * @param maxLines
	 * @param colKey
	 * @param colValue
	 * @param columnDelimiter
	 * @param header
	 * @return  The B+-tree index (in memory).
	 */
/*	
	public BPlusTree<Double, String> buildIndex(String inputFile, int maxLines, int colKey, int colValue, String columnDelimiter, boolean header, PrintStream logStream) {

		BPlusTree<Double, String> index = new BPlusTree<Double, String>(64);

		int lineCount = 0, errorLines = 0;
		try {
			BufferedReader br = new BufferedReader(new FileReader(inputFile));
			String line;
			String[] columns;

			// if the file has a header, ignore the first line
			if (header) {
				br.readLine();
			}

			// Consume rows and populate the index
			while ((line = br.readLine()) != null) {
				if (maxLines > 0 && lineCount >= maxLines) {
					break;
				}
				try {
					columns = line.split(columnDelimiter);
					if ((columns[colKey].isEmpty()) || (columns[colValue].isEmpty())) 
						throw new NullPointerException();
					index.insert(Double.parseDouble(columns[colKey]), columns[colValue]);
					lineCount++;
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

		logStream.println("Finished indexing data from file. Items indexed: " + lineCount
				+ ". Lines skipped due to errors: " + errorLines + ".");

		return index;
	}
*/

	/**
	 * Builds a B+-tree index based on (key,value) pairs available in a collection.
	 * CAUTION! Indexes original features without applying normalization.
	 * @param targetData  The collection of data given as (key, value) pairs.
	 * @return A handle to the root of the created B+-tree.
	 */
	public BPlusTree<Double, String> buildIndex(HashMap<String, Double> targetData, Logger log) {

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
	 * Builds a B+-tree index based on (key,value) pairs available in a collection.
	 * The values are normalized according to the specified normalization method (Z-score or unity-based).
	 * @param targetData  The collection of data given as (key, value) pairs.
	 * @param normal  The normalization method to be applied in each value before insertion into the index.
	 * @return A handle to the root of the created B+-tree.
	 */
	public BPlusTree<Double, String> buildNormalizedIndex(HashMap<String, Double> targetData, INormal normal, Logger log) {

		BPlusTree<Double, String> index = new BPlusTree<Double, String>(64);
		
		int lineCount = 0;
		try {
			// Consume data and populate the index
			for (Map.Entry<String, Double> entry : targetData.entrySet()) {
				//CAUTION! Normalized values (doubles) are used as keys for internal nodes in the B+-tree
				index.insert(normal.normalize(entry.getValue()), entry.getKey());
				lineCount++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		log.writeln("Finished indexing normalized data. Items indexed: " + lineCount + ".");

		return index;
	}

	/**
	 * Reading data from a table from a dBMS over a JDBC connection.
	 * @param tableName
	 * @param keyColumnName
	 * @param valColumnName
	 * @param jdbcConnector
	 * @param log
	 * @return
	 */
	public HashMap<String, Double> readFromJDBCTable(String tableName, String keyColumnName, String valColumnName, JdbcConnector jdbcConnector, Logger log) {

		HashMap<String, Double> dict = new HashMap<String, Double>();

		long startTime = System.nanoTime();
		// In case no column for key datasetIdentifiers has been specified, use the primary key of the table  
		if (keyColumnName == null) { 
			// FIXME: Assuming that primary key is a single attribute (column) 
			// FIXME: SQL query to retrieve primary key attribute works for PostgreSQL only
			String sqlPrimaryKey = "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = '" + tableName + "'::regclass AND i.indisprimary;";	 
			keyColumnName = (String) jdbcConnector.findSingletonValue(sqlPrimaryKey);
		}
  	  
	  	ResultSet rs;	
		try { 
			 //Execute SQL query in the DBMS and fetch all values available for this attribute
			 rs = jdbcConnector.executeQuery("SELECT " + keyColumnName + ", " + valColumnName + " FROM " + tableName + ";");		  
			 // Iterate through all retrieved results and put them to the in-memory look-up
		     while (rs.next()) {  
		    	 dict.put(rs.getString(1), rs.getDouble(2));
		      }
		     log.writeln("Extracted data from database table " + tableName + " regarding column " + valColumnName + " in " + (System.nanoTime() - startTime) / 1000000000.0 + " sec.");
		 }
		 catch(Exception e) { 
				log.writeln("An error occurred while retrieving data from the database.");
				e.printStackTrace();
		 }
		 
		 
		 return dict;
	}
	
	/**
	 * Creates a dictionary of (key,value) pairs of all items read from a CSV file
	 * Also calculates aggregate statistics (COUNT, MIN, MAX, AVG, STDEV) over the input collection.
	 * ASSUMPTION: Input data collection consists of pairs of doubles (KEY) and strings (VALUE)
	 * @param inputFile
	 * @param maxLines
	 * @param colKey
	 * @param colValue
	 * @param columnDelimiter
	 * @param header
	 * @param log
	 * @return  A dictionary (i.e., a hash map) of the (key,value) pairs
	 */
	public HashMap<String, Double> readFromCSVFile(String inputFile, int maxLines, int colKey, int colValue, String columnDelimiter, boolean header, Logger log) {

		HashMap<String, Double> dict = new HashMap<String, Double>();

		count = 0;
		avgVal = stDev = 0.0;
		minVal = Double.MAX_VALUE;
		maxVal = Double.MIN_VALUE;
		
		int errorLines = 0;
		try {
			BufferedReader br = new BufferedReader(new FileReader(inputFile));
			String line;
			String[] columns;
			double v;

			// if the file has a header, retain the names of the columns for possible future use
			if (header) {
				line = br.readLine();
				//FIXME: Special handling when delimiter appears in an attribute value enclosed in quotes
				columns = line.split(columnDelimiter+"(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
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
					columns = line.split(columnDelimiter);
					if ((columns[colKey].isEmpty()) || (columns[colValue].isEmpty())) 
						throw new NullPointerException();
					v = Double.parseDouble(columns[colKey]);
					dict.put(columns[colValue], v);
					adjustDistributiveStats(v);      // Update distributive statistics while parsing
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

		log.writeln("Finished reading data from file: " + inputFile + ". Items read: " + count
				+ ". Lines skipped due to errors: " + errorLines + ".");

		// Report statistics
		calculateAlgebraicStats(dict.values());
		log.writeln("Statistics: count: " + count + ", min: " + minVal + ", max: " + maxVal + ", avg: " + avgVal + " , stdev: " + stDev + ".");
		
		return dict;
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