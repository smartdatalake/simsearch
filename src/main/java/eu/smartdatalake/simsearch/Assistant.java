package eu.smartdatalake.simsearch;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.json.simple.JSONArray;

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
	 * @param colName  The attribute name.
	 * @param columnDelimiter  The delimiter character in the CSV file.
	 * @return  An integer >=0 representing the ordering of the column in the file; -1, if this attribute name is not found in the header.
	 */
	public int getColumnNumber(String inputFile, String colName, String columnDelimiter) {

		int index = -1;
		try {
			BufferedReader br = new BufferedReader(new FileReader(inputFile));
			// if the file has a header, identify the names of the columns
			String line = br.readLine();
			//FIXME: Special handling when delimiter appears in an attribute value enclosed in quotes
			String[] columns = line.split(columnDelimiter+"(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			for (int i=0; i< columns.length; i++) {
			    if (columns[i].equals(colName)) {
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
	 * @param colName  The attribute name.
	 * @param jdbcConnector  A connection instance to the database.
	 * @return  An integer >=0 representing the ordering of the column in the table; -1, if this attribute name is not found.
	 */
	public int getColumnNumber(String dataSource, String colName, JdbcConnector jdbcConnector) {

		int index = -1;
		try {		
			// FIXME: This query works only for PostgreSQL databases
			String sql = "SELECT ordinal_position FROM information_schema.columns WHERE table_name ='" + dataSource + "' AND column_name = '" + colName + "'";
//			System.out.println(sql);
			index = (int) jdbcConnector.findSingletonValue(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return index;
	}

	/**
	 * Identifies whether the given column in the database table has an associated index (numerical, spatial, textual).
	 * @param dataSource   The database table.
	 * @param colName  The attribute name.
	 * @param jdbcConnector  A connection instance to the database.
	 * @return  True, if an index exists in this column; otherwise, False.
	 */
	public boolean isJDBCColumnIndexed(String dataSource, String colName, JdbcConnector jdbcConnector) {

		Object indexName = null;
		try {		
			// FIXME: This query works only for PostgreSQL databases
			String sql = "SELECT i.relname AS index_name FROM pg_class t, pg_class i, pg_index ix, pg_attribute a WHERE t.oid = ix.indrelid AND i.oid = ix.indexrelid AND a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) AND t.relkind = 'r' AND t.relname LIKE '" + dataSource + "%' AND a.attname LIKE '" + colName + "%' ORDER BY t.relname, i.relname;";
//			System.out.println(sql);
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
	
}
