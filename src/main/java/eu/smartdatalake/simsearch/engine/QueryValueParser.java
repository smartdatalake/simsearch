package eu.smartdatalake.simsearch.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.locationtech.jts.geom.Geometry;

import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.csv.spatial.LocationReader;

/**
 * Parser for the various types of query values specified for the attributes in similarity search queries.
 */
public class QueryValueParser {

	String delimiter;
	
	/**
	 * Constructor #1 (with default delimiter)
	 */
	public QueryValueParser() {
		
		// Assuming the default delimiter is used
		this.delimiter = Constants.TOKEN_DELIMITER;
	}

	/**
	 * Constructor #2
	 * @param tokenDelimiter  The delimiter to be used in splitting string values.
	 */
	public QueryValueParser(String tokenDelimiter) {
		
		this.delimiter = tokenDelimiter;
	}

	
	/**
	 * Parsing of a string or a string array (e.g., keywords).
	 * @param val  The input value: a string or a string array.
	 * @return  An array of string values.
	 */
	public String[] parseStringArray(Object val) {
		
		String[] elements = null;
		
		if (val instanceof ArrayList) {
			// At least one element (e.g., keyword) should be specified
			ArrayList<String> arrElements = (ArrayList) val;
			if (!arrElements.isEmpty()) {
				elements = (String[]) (arrElements).toArray(new String[(arrElements).size()]);
			}	
		}
		else if (val instanceof String) {		
			elements = ((String) val).split(delimiter);
		}

		return elements;
	}
	
	
	/**
	 * Parsing a string value (e.g., a WKT representation) into a geometry.
	 * @param val  The input value, typically a 2-dimensional WKT representation like "Point (-12.67 43.68)".
	 * @return  A 2-dimensional geometry, usually a point location.
	 */
	public Geometry parseGeometry(Object val) {
		
		String queryWKT = String.valueOf(val);
		LocationReader locReader = new LocationReader();
		return locReader.WKT2Geometry(queryWKT);
	}
	
	
	/**
	 * Parsing the sequence of coordinates of a possibly multi-dimensional point (used in PIVOT-based search).
	 * @param val  The value to be parsed: an array of values, a string, a number, or a WKT-like representation of a point, e.g., "Point (-12.67 43.68 2.78 -127.52)".
	 * @return  An array of string values.
	 */
	public String[] parseCoordinates(Object val) {

		String q;
		if (val == null)  {
			// Handle NULL values (i.e., missing query values in attributes)
			return new String[0];
		}
		else if (val instanceof ArrayList) {
			// Parse incoming array of values into a concatenated string
			q = String.join(delimiter, (List<String>) ((ArrayList)val).stream().map(Object::toString).collect(Collectors.toList())); //(ArrayList)val);
		}
		else if (val instanceof Number) {
			// This is a single numerical value
			q = "" + val;
		}
		else {  // Parse incoming value as a string
			q = String.valueOf(val);
			// Handle cases of WKT-like representations of multi-dimensional points
			if (((String) val).toUpperCase().contains("POINT (") || ((String) val).toUpperCase().contains("POINT("))
				q = ((String) val).toUpperCase().replace("POINT", "").replace("(", "").replace(")", "").trim().replace(" ", delimiter);
			}

		return q.split(delimiter);
	}
	
}
