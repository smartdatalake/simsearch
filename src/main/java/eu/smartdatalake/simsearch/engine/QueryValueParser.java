package eu.smartdatalake.simsearch.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Stream;

import org.locationtech.jts.geom.Geometry;

import eu.smartdatalake.simsearch.Assistant;
import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.manager.DataType;
import eu.smartdatalake.simsearch.manager.DataType.Type;
import eu.smartdatalake.simsearch.manager.ingested.spatial.LocationReader;

/**
 * Parser for the various types of query values specified for the attributes in similarity search queries.
 */
public class QueryValueParser {

	String delimiter;
	private Type dtype;
	Assistant myAssistant;
	
	/**
	 * Constructor #1 (using the default delimiter)
	 */
	public QueryValueParser() {
		
		myAssistant = new Assistant();		
		// Assuming the default delimiter is applied
		this.delimiter = Constants.TOKEN_DELIMITER;
	}

	/**
	 * Constructor #2
	 * @param tokenDelimiter  The user-specified delimiter to be applied in splitting string values.
	 */
	public QueryValueParser(String tokenDelimiter) {
		
		myAssistant = new Assistant();	
		this.delimiter = tokenDelimiter;
	}
	
	
	/**
	 * Parsing a string value (e.g., a WKT representation) into a geometry.
	 * @param val  The input value, typically a 2-dimensional WKT representation like "Point (-12.67 43.68)".
	 * @return  A 2-dimensional geometry, usually a point location.
	 */
	public Geometry parseGeometry(Object val) {
		
		dtype = DataType.Type.GEOLOCATION;
		String queryWKT = String.valueOf(val);
		// Check for empty input string
		if ((queryWKT == null) || (queryWKT.trim().isEmpty()))
			return null;
		LocationReader locReader = new LocationReader();
		Geometry g = locReader.WKT2Geometry(queryWKT);
		// If query location is not in WKT, then try to parse it as an array or string
		if (g == null) {
			double[] coords;
			Object res = parse(val);
			if (res instanceof String[])
				coords = Arrays.stream((String[]) res).mapToDouble(Double::parseDouble).toArray();
			else 
				coords = Stream.of((Double[]) res).mapToDouble(Double::doubleValue).toArray();
			g = locReader.LonLat2Geometry(coords[0], coords[1]);
		}
		
		return g;
	}
	


	/**
	 * Custom parsing of date/time values into epoch (double) values.
	 * @param val  The input date/time value.
	 * @return  A double representing the epoch (milliseconds in the decimal part).
	 */
	public Double parseDate(Object val) {
		
		dtype = DataType.Type.UNKNOWN;
		Double d = null;

		if ((d = myAssistant.getEpoch("" + val)) != null) {
			dtype = DataType.Type.DATE_TIME;  
		}
	
		return d;  // CAUTION! a numerical epoch value to be used in query evaluation	
	}

	

	/**
	 * Generic parser for various types of query values: sets of keywords, sets of coordinates, numbers, or strings.
	 * Also used in parsing the sequence of coordinates of a possibly multi-dimensional point (used in PIVOT-based search).
	 * @param val  The value to be parsed: an array of values, a string, a number, or a WKT-like representation of a point, e.g., "Point (-12.67 43.68 2.78 -127.52)".
	 * @return  An object with a proper representation of query values for querying an attribute.
	 */
	public Object parse(Object val) {

		dtype = DataType.Type.UNKNOWN;
		String q;
		Double d;
		
		if (val == null)  {
			// Handle NULL values (i.e., missing query values in attributes)
			return null;
		}
		else if (val instanceof ArrayList<?>) {
			// At least one element (e.g., keyword) should be specified
			ArrayList<?> elements = (ArrayList<?>) val;
			if (!elements.isEmpty()) {
				if (elements.get(0) instanceof Number) {	// Array of numbers
					dtype = DataType.Type.NUMBER_ARRAY;
					return elements.toArray(new Double[elements.size()]);
				}
				else {	// Array of strings, also trimmed from spaces between keywords
					dtype = DataType.Type.KEYWORD_SET;
					return Arrays.stream(elements.toArray(new String[elements.size()])).map(String::trim).toArray(String[]::new);
				}
			}
			else
				return null;
		}
		else if (val instanceof Number) {
			// This is a single numerical value
			q = "" + val;
			dtype = DataType.Type.NUMBER;
			//TODO: Return numeric value to be used?
			//return val;
		}
		else {  // Parse incoming value as a string without white space
			q = String.valueOf(val).replaceAll("\\s{2,}", " ").trim();		
			if (q.isEmpty())
				return null;
			// Handle cases of WKT-like representations of multi-dimensional points
			else if (q.toUpperCase().startsWith("POINT (") || q.toUpperCase().startsWith("POINT(")) {
				// Keep only coordinate values 
				q = q.toUpperCase().replace("POINT", "").replace("(", "").replace(")", "").trim().replace(" ", delimiter);
				dtype = DataType.Type.GEOLOCATION;
			}	
			else if (myAssistant.isNumeric(q)) {
				dtype = DataType.Type.NUMBER;
				// TODO: Return numeric value to be used?
//				return Double.parseDouble(q);
			}
			else if ((d = myAssistant.getEpoch(q)) != null) {   // Parse date/time into epoch
				dtype = DataType.Type.DATE_TIME;
				return d;   // CAUTION! a numerical value to be used in query evaluation
			}
			else {
				dtype = DataType.Type.KEYWORD_SET;
				if (q.startsWith("[") && q.endsWith("]")) // Expunge brackets
					q = q.substring(1, q.length()-1);
			}
		}
	
		// Trim string values in the resulting array
		return Arrays.stream(q.split(delimiter)).map(String::trim).toArray(String[]::new);
	}
	
	
	/** 
	 * Provides the data type of the query value as determined from parsing.
	 * @return  Once of the data types supported for queryable attributes.
	 */
	public Type getDataType() {
		return dtype;
	}
	
}
