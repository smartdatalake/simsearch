package eu.smartdatalake.simsearch;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.time.Instant;
import java.time.Year;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;
import java.util.stream.IntStream;

import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.json.simple.JSONArray;

import eu.smartdatalake.simsearch.manager.DataType.Type;
import eu.smartdatalake.simsearch.manager.ingested.DataFileReader;
import eu.smartdatalake.simsearch.manager.ingested.categorical.TokenSet;
import eu.smartdatalake.simsearch.manager.ingested.temporal.DateTimeParser;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Point;

/**
 * Auxiliary class that provides various helper methods.
 */
public class Assistant {

	/**
	 * Constructor
	 */
	public Assistant() {
		
	}
	
	
	/**
	 * Provides the textual description of the type of operation with the given number.
	 * @param op  The numeric identifier of the requested operation (e.g., 0: CATEGORICAL_TOPK; 1:SPATIAL_KNN ; 2:NUMERICAL_TOPK).
	 * @return  A string describing the type of the operation.
	 */
	public String decodeOperation(int op) {
		
		switch(op) {
		case Constants.CATEGORICAL_TOPK:
			return "categorical_topk";
		case Constants.SPATIAL_KNN:   
			return "spatial_knn";
		case Constants.NUMERICAL_TOPK:   
			return "numerical_topk";
		case Constants.TEMPORAL_TOPK:   
			return "temporal_topk";
		case Constants.PIVOT_BASED:   
			return "pivot_based";
		case Constants.NAME_DICTIONARY:   
			return "name_dictionary";
		case Constants.KEYWORD_DICTIONARY:   
			return "keyword_dictionary";
		case Constants.VECTOR_DICTIONARY:   
			return "vector_dictionary";
		case Constants.TEXTUAL_TOPK:
			return "textual_topk";
		default:
			return "unknown operation";
		}
	}
	

	/**
	 * Identifies which column in the header of the input CSV file corresponds to the given attribute name.
	 * @param inputFile   The input CSV file.
	 * @param colName  The attribute name.
	 * @param columnDelimiter  The delimiter character in the CSV file.
	 * @return  A positive integer representing the ordinal number of the column in the file; -1, if this attribute name is not found in the header.
	 */
	public int getColumnNumber(String inputFile, String colName, String columnDelimiter) {

		int index = -1;
		String col = colName;
		try {
			// In case multiple columns are specified (e.g., lon/lat coordinates), the first column is used for identification
			if (colName.startsWith("[") && colName.endsWith("]")) {
				String[] columns = colName.substring(1, colName.length()-1).replace("\"", "").split(",");
				col = columns[0];
			}
			
			// Custom reader to handle either local or remote CSV files
			DataFileReader br = new DataFileReader(inputFile);
			// This file has a header, so identify the names of the columns in its first line
			String line = br.readLine();
			// FIXME: Custom handling when delimiter appears in an attribute name enclosed in quotes
			String[] columns = line.split(columnDelimiter+"(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			for (int i=0; i< columns.length; i++) {
			    if (columns[i].equals(col)) {
			        index = i;
			        break;
			    }
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return index;
	}


	/**
	 * Converts a JSONArray object into an array of strings.
	 * @param jsonArray  Input JSON array.
	 * @return  Output array of string values.
	 */
	public String[] arrayJSON2String(JSONArray jsonArray) {
		
		String[] stringArray = null;
		if (jsonArray != null) {
			stringArray = new String[jsonArray.size()];
			for (int i = 0; i < jsonArray.size(); i++) {
			    stringArray[i] = (String) jsonArray.get(i);
			}
		}
		
		return stringArray;
	}
	
	
	/**
	 * Provides an array of double numbers as consumed from a JSON array of values.
	 * @param jsonArray   A JSON array of values.
	 * @return   Output array of double values.
	 */
	public Double[] arrayJSON2Double(JSONArray jsonArray) {
		
		Double[] doubleArray = null;		
		if (jsonArray != null) {
			doubleArray = new Double[jsonArray.size()];
			for (int i = 0; i < jsonArray.size(); i++) {
				doubleArray[i] = Double.parseDouble(String.valueOf(jsonArray.get(i)));
			}
		}
		
		return doubleArray;
	}

	
	/**
	 * Formats attribute values to be issued in the reporting results.
	 * @param val  The attribute value (numerical, textual, spatial) to be formatted.
	 * @return  The formatted string.
	 */
	public String formatAttrValue(Object val) {

		// Special handling of numeric formats
		if (isNumeric(val.toString())) {
			double num = Double.parseDouble(val.toString());
			if (num == Math.rint(num))   	// integer
				return Long.toString((long) num);
		    return String.valueOf(num); 	// double 
		}
		else if (val instanceof String[])
			return Arrays.toString((String[]) val);
		else	// other data type
			return val.toString();
	}

	
	/**
	 * Formats date/time values to be issued in the reporting results.
	 * @param epoch  The attribute value expressed as a double number (epoch).
	 * @return  The formatted date/time.
	 */
	public String formatDateValue(Object epoch) {
		
		Long millis = (long) (Double.valueOf(epoch.toString())*1000);
		// FIXME: If no time zone is set in the data, it return times in UTC
		Instant instant = Instant.ofEpochMilli(millis);   
		
		return instant.toString();
	}
	
	/**
	 * Format entity identifiers as URL for the final results
	 * @param prefixURL  The HTTP prefix to be used.
	 * @param id  The identifier of the entity
	 * @return  A resolvable URL to be used as entity identifier for a similarity search result.
	 */
	public String formatURL(String prefixURL, String id) {
		
		return (prefixURL + id.substring(0, 12));             // FIXME: Custom handling for ATOKA identifiers using only the first 12 characters in identifiers
	}

	
	/**
	 * Tokenizes the given string of keywords using a specific character as delimiter.
	 * FIXME: This method is also specified in the CategoricalValueFinder class.	
	 * @param id   Identifier of this collection of keywords.
	 * @param keywords   A string containing keywords.
	 * @param tokDelimiter   The delimiter character between keywords.
	 * @return  A set of tokens (keywords).
	 */
	public TokenSet tokenize(String id, String keywords, String tokDelimiter) {
		
		TokenSet set = null;
		try {
			if (keywords != null) {
				set = new TokenSet();
				set.id = id; 
				set.tokens = new ArrayList<String>();
				// Split and trim keywords
				List<String> tokens = new ArrayList<String>(new HashSet<String>(Arrays.asList(keywords.split("\\s*" + tokDelimiter + "\\s*"))));
				set.tokens.addAll(tokens);
//				tokens.forEach(System.out::println);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return set;
	}

	
	/**
	 * Tokenizes the given string value using qgrams.	
	 * @param id   Identifier of this collection of tokens.
	 * @param value   A string value.
	 * @param qgram  The qgram used (if applicable for string similarity).
	 * @return  A set of tokens derived from the original string.
	 */
	public TokenSet tokenize(String id, String value, int qgram) {
		
		TokenSet set = null;
		try {
			if (value != null) {
				set = new TokenSet();
				set.id = id; 
				set.tokens = new ArrayList<String>();
				if (qgram > 0) {  // Creates qgrams from a single input value
					set.originalString = value;
					Reader reader = new StringReader(value);
					NGramTokenizer gramTokenizer = new NGramTokenizer(reader, qgram, qgram);
					CharTermAttribute charTermAttribute = gramTokenizer.addAttribute(CharTermAttribute.class);
					while (gramTokenizer.incrementToken()) {
						set.tokens.add(charTermAttribute.toString());
					}
					gramTokenizer.end();
					gramTokenizer.close();
				}
//				tokens.forEach(System.out::println);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return set;
	}
	
	
	/**
	 * Creates a d-dimensional point with zero values in all coordinates
	 * @param d  The dimensionality of the point.
	 * @return  A d-dimensional point with zero values in all coordinates.
	 */
	public Point createZeroPoint(int d) {
		double [] coords = new double[d];
		Arrays.fill(coords, 0.0);
		Point p = Point.create(coords);  
		return p;
	}
	
	
	/**
	 * Creates a d-dimensional point with NaN values in all coordinates
	 * @param d  The dimensionality of the point.
	 * @return  A d-dimensional point with NaN values in all coordinates.
	 */
	public Point createNaNPoint(int d) {
		double [] coords = new double[d];
		Arrays.fill(coords, Double.NaN);
		Point p = Point.create(coords);  
		return p;
	}
	

	/**
	 * Applies a given operation (e.g., SUM) over two input arrays of double numbers value per value.
	 * @param operator  The operator to apply over the two arrays element-wise.
	 * @param a  The first array of double values.
	 * @param b  The first array of double values.
	 * @return  The resulting array of double values.
	 */
	public double[] applyOnDoubleArrays(DoubleBinaryOperator operator, double[] a, double b[]) {
		
		return IntStream.range(0, a.length)
				.mapToDouble(index -> operator.applyAsDouble(a[index], b[index]))
                .toArray();
    }

	
	/**
	 * Applies a given operation (e.g., division by a number) over each value of an input array of double numbers.
	 * @param operator  The operator to apply over the elements of this array.
	 * @param a  The input array of double values.
	 * @return  The resulting array of double values.
	 */
	public double[] applyOnDoubleArray(DoubleUnaryOperator operator, double[] a) {
		
		return IntStream.range(0, a.length)
				.mapToDouble(index -> operator.applyAsDouble(a[index]))
                .toArray();
    }
	
	
	/**
	 * Checks whether the given string value represents a valid number.
	 * @param str  The string value to check.
	 * @return  True, if the string represents a valid number; otherwise, False.
	 */
	public boolean isNumeric(String str) {
	    if (str == null) 
	        return false;

	    // Validate number
	    try {
	        double d = Double.parseDouble(str);
	    } catch (NumberFormatException e) {
	        return false;
	    }
	    return true;
	}
	
	
	/**
	 * Checks whether the given string value represents a valid date/time.
	 * @param str  The string value to check.
	 * @return  True, if the string represents a valid date/time; otherwise, False.
	 */
	public boolean isDateTime(String str) {
		return getEpoch(str) != null;
	}
	
	
	/**
	 * Checks whether the given string value represents a valid year.
	 * @param str  The string value to check.
	 * @return  True, if the string represents a valid year; otherwise, False.
	 */
	public boolean isYear(String str) {
		try {
			// Check if this is a year value
			Year year = Year.of(Integer.parseInt(str));		
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
	
	/**
	 * Parses the given string to extract a date/time value.
	 * @param str  A string containing a date/time value.
	 * @return  A double number representing the equivalent UNIX epoch (milliseconds in the decimal part).
	 */
	public Double getEpoch(String str) {
	    if (str == null)
	        return null;

    	// Validate date/time
    	DateTimeParser parser = new DateTimeParser();
    	Double d = parser.parseDateTimeToEpoch(str);

	    return d;
	}
	
	/**
	 * Randomly pick a value from a dataset of (key, value) pairs.
	 * @param targetData  The dataset, typically retaining attribute values.
	 * @param dtype  The data type of the values.
	 * @return  A randomly chosen value from the dataset.
	 */
	public String pickRandomValue(Map<String,?> targetData, Type dtype) {
		Random random = new Random();
		List<String> keys = new ArrayList<String>(targetData.keySet());
		if (keys.size() == 0)   // No data available
			return null;
		String randomKey = keys.get(random.nextInt(keys.size()));
		if (dtype == Type.DATE_TIME)
			return formatDateValue(targetData.get(randomKey));
		else
			return formatAttrValue(targetData.get(randomKey));
	}
}
