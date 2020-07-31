package eu.smartdatalake.simsearch;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.json.simple.JSONArray;

import eu.smartdatalake.simsearch.csv.categorical.TokenSet;
import eu.smartdatalake.simsearch.jdbc.JdbcConnector;

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
	 * @param op  The numeric identifier of the requested operation (0: CATEGORICAL_TOPK; 1:SPATIAL_KNN ; 2:NUMERICAL_TOPK).
	 * @return  A string describing the type of the operation.
	 */
	public String descOperation(int op) {
		
		switch(op) {
		case Constants.CATEGORICAL_TOPK:
			return "categorical_topk";
		case Constants.SPATIAL_KNN:   
			return "spatial_knn";
		case Constants.NUMERICAL_TOPK:   
			return "numerical_topk";
		default:
			return "unknown operation";
		}
	}
	

	/**
	 * Identifies which column in the header of the input CSV file corresponds to the given attribute name.
	 * @param inputFile   The input CSV file.
	 * @param attrValue  The attribute name.
	 * @param columnDelimiter  The delimiter character in the CSV file.
	 * @return  An integer >=0 representing the ordering of the column in the file; -1, if this attribute name is not found in the header.
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
			
			BufferedReader br = new BufferedReader(new FileReader(inputFile));
			// if the file has a header, identify the names of the columns
			String line = br.readLine();
			//FIXME: Special handling when delimiter appears in an attribute value enclosed in quotes
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
	 * Identifies which column in the database table corresponds to the given attribute name.
	 * @param dataSource   The database table.
	 * @param attrValue  The attribute name.
	 * @param jdbcConnector  A connection instance to the database.
	 * @return  An integer >=0 representing the ordering of the column in the table; -1, if this attribute name is not found.
	 */
	public int getColumnNumber(String dataSource, String colName, JdbcConnector jdbcConnector) {

		int index = -1;
		String col = colName;
		try {	
			// In case multiple columns are specified (e.g., lon/lat coordinates), the first column is used for identification
			if (colName.startsWith("[") && colName.endsWith("]")) {
				String[] columns = colName.substring(1, colName.length()-1).replace("\"", "").split(",");
				col = columns[0];
			}
			
			// Check if column is available according to DBMS specifications
			String sql = null;		
			switch(jdbcConnector.getDbSystem()) {
			case "POSTGRESQL":   	// Connected to PostgreSQL
				sql = "SELECT ordinal_position FROM information_schema.columns WHERE table_name ='" + dataSource + "' AND column_name = '" +  col + "'";
				index = (int) jdbcConnector.findSingletonValue(sql);
				break;
			case "AVATICA":   		// Connected to Avatica JDBC (Proteus)
				// TODO: How else to examine existence of a specific column in Proteus?
				sql = "SELECT count(*) FROM (SELECT " + col + " FROM " + dataSource + " WHERE " + col + " IS NOT NULL LIMIT 1) test";
				index = Integer.parseInt((String) jdbcConnector.findSingletonValue(sql));
				break;
	        default:
	        	sql = null;
	        } 
//			System.out.println(sql);	
//			System.out.println("COLUMN: " + index);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return index;
	}

	
	/**
	 * Identifies whether the given column in the database table has an associated index (numerical, spatial, textual).
	 * @param dataSource   The database table.
	 * @param attrValue  The attribute name.
	 * @param jdbcConnector  A connection instance to the database.
	 * @return  True, if an index exists in this column; otherwise, False.
	 */
	public boolean isJDBCColumnIndexed(String dataSource, String colName, JdbcConnector jdbcConnector) {

		Object indexName = null;
		try {		
			// Check if column is available according to DBMS specifications
			String sql = null;		
			switch(jdbcConnector.getDbSystem()) {
			case "POSTGRESQL":  // Connected to PostgreSQL
				sql = "SELECT i.relname AS index_name FROM pg_class t, pg_class i, pg_index ix, pg_attribute a WHERE t.oid = ix.indrelid AND i.oid = ix.indexrelid AND a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) AND t.relkind = 'r' AND t.relname LIKE '" + dataSource + "%' AND a.attname LIKE '" + colName + "%' ORDER BY t.relname, i.relname;";
				break;
			case "AVATICA":   // FIXME: How to examine if a specific column is indexed in Avatica JDBC connections (Proteus)?
				sql = "SELECT TRY_CAST(" + colName + " AS double) FROM " + dataSource + " WHERE " + colName + " IS NOT NULL LIMIT 1";   // Work-around to identify queryable numerical attributes
//				sql = null;
//				indexName = "idxByProteus";   // FIXME: Placeholder to indicate that no data from Proteus need be ingested
				break;
	        default:
	        	sql = null;
	        } 
//			System.out.println(sql);
			if (sql != null)
				indexName = jdbcConnector.findSingletonValue(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return (indexName != null);
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
	 * Formats attribute values to be issued in the reporting results
	 * @param val  The attribute value (numerical, textual, spatial) to be formatted.
	 * @return  The formatted string.
	 */
	public String formatAttrValue(Object val) {
		
		// Special handling of numeric formats
		if (val instanceof Double) {
			double num = (double) val;
		    if ((long) num == num) 
		    	return Long.toString((long) num);
		    return String.valueOf(num);
		  
		}
		else
			return val.toString();
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
	
}
