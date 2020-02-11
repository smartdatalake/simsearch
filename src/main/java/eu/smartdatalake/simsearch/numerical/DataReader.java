package eu.smartdatalake.simsearch.numerical;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Consumes data from a CSV file and extracts numerical values from a specific attribute.
 * Values are maintained in a hash table taking their keys from another attribute in the file. 
 * Aggregate statistics are also collected while parsing the file.
 * A B+-tree index may be created from the collected (key,value) pairs.
 */
public class DataReader {

	// Basic statistics about the numeric values read in the input collection
	public double minVal, maxVal, avgVal, stDev;
	public int count;


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
	public BPlusTree<Double, String> buildIndex(String inputFile, int maxLines, int colKey, int colValue,
			String columnDelimiter, boolean header, PrintStream logStream) {

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
	public BPlusTree<Double, String> buildIndex(HashMap<String, Double> targetData, PrintStream logStream) {

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

		logStream.println("Finished indexing data. Items indexed: " + lineCount + ".");

		return index;
	}


	/**
	 * Builds a B+-tree index based on (key,value) pairs available in a collection.
	 * The values are normalized according to the specified normalization method (Z-score or unity-based).
	 * @param targetData  The collection of data given as (key, value) pairs.
	 * @param normal  The normalization method to be applied in each value before insertion into the index.
	 * @return A handle to the root of the created B+-tree.
	 */
	public BPlusTree<Double, String> buildNormalizedIndex(HashMap<String, Double> targetData, INormal normal, PrintStream logStream) {

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

		logStream.println("Finished indexing normalized data. Items indexed: " + lineCount + ".");

		return index;
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
	 * @return  A dictionary (i.e., a hash map) of the (key,value) pairs
	 */
	public HashMap<String, Double> read(String inputFile, int maxLines, int colKey, int colValue,
			String columnDelimiter, boolean header, PrintStream logStream) {

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

			// if the file has a header, ignore the first line
			if (header) {
				br.readLine();
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

		logStream.println("Finished reading data from file. Items read: " + count
				+ ". Lines skipped due to errors: " + errorLines + ".");

		// Report statistics
		calculateAlgebraicStats(dict.values());
		logStream.println("Statistics: count: " + count + ", min: " + minVal + ", max: " + maxVal + ", avg: " + avgVal + " , stdev: " + stDev + ".");
		
		return dict;
	}
	
}
