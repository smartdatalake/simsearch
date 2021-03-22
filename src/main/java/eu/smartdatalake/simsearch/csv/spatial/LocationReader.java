package eu.smartdatalake.simsearch.csv.spatial;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;

import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.csv.DataFileReader;
import eu.smartdatalake.simsearch.jdbc.JdbcConnector;

/**
 * Consumes data from a CSV file and extracts location values from two specific attributes.
 * Locations (as geometries) are maintained in a hash table taking their keys from another attribute in the file. 
 * An R-tree index may be created from the collected (key,location) pairs.
 */
public class LocationReader {
	
	/** 
	 * Returns a binary geometry representation according to its Well-Known Text serialization.
	 * @param wkt  WKT of the geometry.
	 * @return  A geometry object.
	 */
	public Geometry WKT2Geometry(String wkt) {  
	  	    
		WKTReader wktReader = new WKTReader();
		Geometry g = null;
        try {
        	g = wktReader.read(wkt);
		} catch (Exception e) {
//			e.printStackTrace();
			return null;
		}
    
        return g;     //Return geometry
	}
	
	/**
	 * Provides a binary geometry representation from the given WGS84 lon/lat geographical coordinates.
	 * @param lon  A double value representing the longitude of the location.
	 * @param lat  A double value representing the latitude of the location.
	 * @return  The geometry representation (in WGS84) of the given location.
	 */
	public Geometry LonLat2Geometry(double lon, double lat) {  
  	    
		WKTReader wktReader = new WKTReader();
		Geometry g = null;
        try {
        	g = wktReader.read("POINT (" + lon + " " + lat + ")");
		} catch (Exception e) {
//			e.printStackTrace();
			return null;
		}
    
        return g;     //Return geometry
	}
	
	
	/**
	 * Creates an R-tree index from the input collection of geometry locations.
	 * @param targetData  A dictionary of all geometries with their unique identifiers as keys.
	 * @param log  Handle to the logger of statistics calculated over the data.
	 * @return  The R-tree index created over the input geometries.
	 */
	public RTree<String, Location> buildIndex(Map<String, Geometry> targetData, Logger log) {

		RTree<String, Location> index = new RTree<String, Location>();
		
		int locCount = 0;
		try {
			// Consume data and populate the index
			for (Map.Entry<String, Geometry> entry : targetData.entrySet()) {
				// R-tree indexes the geometries of objects
				index.insert(new Location(entry.getKey(), entry.getValue()));
				locCount++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			index.finalize();    // IMPORTANT! At this stage the index is finally built, after all data has been inserted
		}

		log.writeln("Finished indexing locations. Items indexed: " + locCount + ".");

		return index;
	}
	
	
	/**
	 * Creates a dictionary of (key,geometry) pairs of all items read from a CSV file.
	 * ASSUMPTION: Input data collection only contains POINT locations referenced in WGS84.
	 * @param inputFile  Path to the input CSV file or its URL at a remote server containing the attribute data.
	 * @param maxLines  Instructs reader to only consume the first lines up to this limit.
	 * @param colKey  An integer representing the ordinal number of the attribute containing the key of the entities.
	 * @param colValue  An integer representing the ordinal number of the attribute containing the values (i.e., geometries) of the entities.
	 * @param columnDelimiter  The delimiter character used between attribute values in the CSV file.
	 * @param header  Boolean indicating whether the first line contains attribute names.
	 * @param log  Handle to the logger for statistics and issues over the input data.
	 * @return  A hash map of (key,geometry) values.
	 */
	public HashMap<String, Geometry> readFromCSVFile(String inputFile, int maxLines, int colKey, int colValue, String columnDelimiter, boolean header, Logger log) {

		HashMap<String, Geometry> dict = new HashMap<String, Geometry>();
		
		// FIXME: Special handling when delimiter appears in an attribute value enclosed in quotes
        String otherThanQuote = " [^\"] ";
        String quotedString = String.format(" \" %s* \" ", otherThanQuote);
        String regex = String.format("(?x) "+ columnDelimiter + "(?=(?:%s*%s)*%s*$)", otherThanQuote, quotedString, otherThanQuote);
        
		int count = 0;
		int errorLines = 0;
		try {
			// Custom reader to handle either local or remote CSV files
			DataFileReader br = new DataFileReader(inputFile);
			String line;
			String[] columns;
			Geometry g;

			// If the file has a header, ignore it
			if (header) {
				line = br.readLine();
			}

			// Consume rows and populate the index
			while ((line = br.readLine()) != null) {
				if (maxLines > 0 && count >= maxLines) {
					break;
				}
				try {  // FIXME: Special handling when delimiter appears in an attribute value enclosed in quotes
					columns = line.split(regex,-1);
					if ((columns[colKey].isEmpty()) || (columns[colValue].isEmpty()))
						throw new NullPointerException();
					// Generate a geometry object from the input coordinates
					g = WKT2Geometry(columns[colValue]);
					dict.put(columns[colKey], g);
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

		log.writeln("Finished reading data from file:" + inputFile + ". Items read: " + count
				+ ". Lines skipped due to errors: " + errorLines + ".");

		return dict;
	}

	/**
	 * Reading spatial attribute data (POINT locations only) from a table in a dBMS over a JDBC connection.
	 * @param tableName  The name of the table (or view) that contains the input data.
	 * @param keyColumnName  The name of the attribute containing the unique identifier (key) of the entities.
	 * @param longitudeColumnName  The name of the attribute containing the longitude ordinate of the entities.
	 * @param latitudeColumnName  The name of the attribute containing the latitude ordinate of the entities.
	 * @param jdbcConnector  Instance of a JDBC connector to the DBMS where the table resides.
	 * @param log  Logger for statistics and issues over the input data.
	 * @return  A hash map of (key,geometry) values.
	 */
	public HashMap<String, Geometry> readFromJDBCTable(String tableName, String keyColumnName, String longitudeColumnName, String latitudeColumnName, JdbcConnector jdbcConnector, Logger log) {

		HashMap<String, Geometry> dict = new HashMap<String, Geometry>();

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
			 //Execute SQL query in the DBMS and fetch all NOT NULL coordinate values available
			 String sql = "SELECT " + keyColumnName + ", " + longitudeColumnName + ", " + latitudeColumnName + " FROM " + tableName + " WHERE " + longitudeColumnName + " IS NOT NULL AND " + latitudeColumnName + " IS NOT NULL";
//			 System.out.println("SPATIAL query: " + sql);
			 rs = jdbcConnector.executeQuery(sql);		  
			 // Iterate through all retrieved results and put them to the in-memory look-up
		     while (rs.next()) {  
		    	 // Generate a geometry object from the input coordinates
		    	 dict.put(rs.getString(1), LonLat2Geometry(Double.parseDouble(rs.getString(2)), Double.parseDouble(rs.getString(3))));
		    	 n++;
		      }
		     log.writeln("Extracted " + n + " data values from database table " + tableName + " regarding columns " + longitudeColumnName + ", " + latitudeColumnName + " in " + (System.nanoTime() - startTime) / 1000000000.0 + " sec.");
		 }
		 catch(Exception e) { 
				log.writeln("An error occurred while retrieving data from the database.");
				e.printStackTrace();
		 }
		 		 
		 return dict;
	}
	
	
	/**
	 * Creates a dictionary of (key,geometry) pairs of all items read from a CSV file
	 * ASSUMPTION: Input data collection only contains POINT locations referenced in WGS84.
	 * @param inputFile  Path to the input CSV file or its URL at a remote server containing the attribute data.
	 * @param maxLines  Instructs reader to only consume the first lines up to this limit.
	 * @param colKey  An integer representing the ordinal number of the attribute containing the key of the entities.
	 * @param colLongitude  An integer representing the ordinal number of the attribute containing the longitude ordinates of the entities.
	 * @param colLatitude  An integer representing the ordinal number of the attribute containing the latitude ordinates of the entities.
	 * @param columnDelimiter  The delimiter character used between attribute values in the CSV file.
	 * @param header  Boolean indicating whether the first line contains attribute names.
	 * @param log  Logger for statistics and issues over the input data.
	 * @return  A hash map of (key,geometry) values.
	 */
	public HashMap<String, Geometry> readFromCSVFile(String inputFile, int maxLines, int colKey, int colLongitude, int colLatitude, String columnDelimiter, boolean header, Logger log) {

		HashMap<String, Geometry> dict = new HashMap<String, Geometry>();

		// FIXME: Special handling when delimiter appears in an attribute value enclosed in quotes
        String otherThanQuote = " [^\"] ";
        String quotedString = String.format(" \" %s* \" ", otherThanQuote);
        String regex = String.format("(?x) "+ columnDelimiter + "(?=(?:%s*%s)*%s*$)", otherThanQuote, quotedString, otherThanQuote);
        
		int count = 0;
		int errorLines = 0;
		try {
			// Custom reader to handle either local or remote CSV files
			DataFileReader br = new DataFileReader(inputFile);
			String line;
			String[] columns;
			Geometry g;

			// If the file has a header, ignore it
			if (header) {
				line = br.readLine();
			}

			// Consume rows and populate the index
			while ((line = br.readLine()) != null) {
				if (maxLines > 0 && count >= maxLines) {
					break;
				}
				try {
					columns = line.split(regex,-1);
					if ((columns[colKey].isEmpty()) || (columns[colLongitude].isEmpty()) || (columns[colLatitude].isEmpty())) 
						throw new NullPointerException();
					// Generate a geometry object from the input coordinates
					g = LonLat2Geometry(Double.parseDouble(columns[colLongitude]), Double.parseDouble(columns[colLatitude]));
					dict.put(columns[colKey], g);
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

		log.writeln("Finished reading data from file:" + inputFile + ". Items read: " + count
				+ ". Lines skipped due to errors: " + errorLines + ".");

		return dict;
	}
	
}
