package eu.smartdatalake.simsearch.ranking.randomaccess;

import java.util.Map;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.postgis.PGgeometry;

import eu.smartdatalake.simsearch.engine.IValueFinder;
import eu.smartdatalake.simsearch.manager.IDataConnector;

/**
 * Updates an in-memory lookup with SPATIAL values used in random access operations during rank aggregation.
 * This method inserts the spatial attribute value found for a given object identifier (primary key) in a data source collection (DBMS table of REST API data).  
 * @param <K>  The key (identifier) of the object.
 * @param <V>  The spatial (point) value of this object at a particular attribute.
 */
public class SpatialValueFinder<K,V> implements IValueFinder<K,V> {

	String queryTemplate = null;
	IDataConnector dataConnector = null;
	WKTReader wktReader = null;             // Converts a WKT geometry into its binary representation
	
	/**
	 * Constructor of this class
	 * @param dataConnector  Instance of a connector to data source (JDBC connection to a DBMS or HTTP connection to a REST API).
	 * @param query  The query template to execute, either in SQL (against a DBMS) or JSON (against a REST API).
	 */
	public SpatialValueFinder(IDataConnector dataConnector, String query) {
		
		this.dataConnector = dataConnector;
		this.queryTemplate = query;
		this.wktReader = new WKTReader();
	}
	
	@Override
	public Geometry find(Map<K,V> dataset, K k) {
		
		// Replace identifier in the query template and execute
		Object val = dataConnector.findSingletonValue(queryTemplate.replace("$id", k.toString()));
		String wkt = null;	
		
		try {	
			if (val instanceof PGgeometry) {	 // Special handling of PostGIS geometry representations that may include EPSG specification
//				System.out.println("GEOM: " + val.toString());
				wkt = val.toString().substring(val.toString().indexOf(";") + 1);
			}
			else if (val != null) {      
				// FIXME: Geo-points in ElasticSearch are expressed as a string with the format: "lat, lon"
				String[] coords = String.valueOf(val).split(",");
	        		wkt = "POINT (" + coords[1] + " " + coords[0] + ")";   // Reverse coordinates for WKT representation
			}
			if (wkt != null) {
//				System.out.println(wkt);
				val = wktReader.read(wkt);
				// Insert geometry into the lookup
				dataset.put(k, (V)val);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return (Geometry)val;
	}

}
