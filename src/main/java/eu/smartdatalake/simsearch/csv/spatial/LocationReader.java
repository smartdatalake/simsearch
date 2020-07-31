package eu.smartdatalake.simsearch.csv.spatial;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;

import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.jdbc.JdbcConnector;

/**
 * Consumes data from a CSV file and extracts location values from two specific attributes.
 * Locations (as geometries) are maintained in a hash table taking their keys from another attribute in the file. 
 * An R-tree index may be created from the collected (key,location) pairs.
 */
public class LocationReader {
	
	/** 
	 * Returns a binary geometry representation according to its Well-Known Text serialization.
	 * @param wkt  WKT of the geometry
	 * @return  A geometry object
	 */
	public Geometry WKT2Geometry(String wkt) {  
	  	    
		WKTReader wktReader = new WKTReader();
		Geometry g = null;
        try {
        	g = wktReader.read(wkt);
		} catch (Exception e) {
			e.printStackTrace();
		}
    
        return g;     //Return geometry
	}
	
	/**
	 * Provides a binary geometry representation from the given WGS84 lon/lat geographical coordinates.
	 * @param lon
	 * @param lat
	 * @return
	 */
	public Geometry LonLat2Geometry(double lon, double lat) {  
  	    
		WKTReader wktReader = new WKTReader();
		Geometry g = null;
        try {
        	g = wktReader.read("POINT (" + lon + " " + lat + ")");
		} catch (Exception e) {
			e.printStackTrace();
		}
    
        return g;     //Return geometry
	}
	
	
	/**
	 * Creates an R-tree index from the input collection of geometry locations.
	 * @param targetData
	 * @param log
	 * @return
	 */
	public RTree<String, Location> buildIndex(HashMap<String, Geometry> targetData, Logger log) {

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
	 * Creates a dictionary of (key,geometry) pairs of all items read from a CSV file
	 * ASSUMPTION: Input data collection only contains POINT locations referenced in WGS84.
	 * @param inputFile
	 * @param maxLines
	 * @param colKey
	 * @param colValue
	 * @param columnDelimiter
	 * @param header
	 * @param log
	 * @return
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
			BufferedReader br = new BufferedReader(new FileReader(inputFile));
			String line;
			String[] columns;
			Geometry g;

			// if the file has a header, ignore it
			if (header) {
				line = br.readLine();
			}

			// Consume rows and populate the index
			while ((line = br.readLine()) != null) {
				if (maxLines > 0 && count >= maxLines) {
					break;
				}
				try {  //FIXME: Special handling when delimiter appears in an attribute value enclosed in quotes
					columns = line.split(regex,-1);
					if ((columns[colKey].isEmpty()) || (columns[colValue].isEmpty()))
						throw new NullPointerException();;
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
	 * @param tableName
	 * @param keyColumnName
	 * @param longitudeColumnName
	 * @param latitudeColumnName
	 * @param jdbcConnector
	 * @param log
	 * @return
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
	 * @param inputFile
	 * @param maxLines
	 * @param colKey
	 * @param colLongitude
	 * @param colLatitude
	 * @param columnDelimiter
	 * @param header
	 * @param log
	 * @return
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
			BufferedReader br = new BufferedReader(new FileReader(inputFile));
			String line;
			String[] columns;
			Geometry g;

			// if the file has a header, ignore it
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
