package eu.smartdatalake.simsearch.jdbc;

import java.util.HashMap;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.postgis.PGgeometry;

import eu.smartdatalake.simsearch.IValueFinder;

/**
 * Updates an in-memory lookup with SPATIAL values used in random access operations during rank aggregation.
 * This method inserts the spatial attribute value found for a given object identifier (primary key) in a database table.  
 * @param <K>  The key (identifier) of the object.
 * @param <V>  The spatial (point) value of this object at a particular attribute.
 */
public class SpatialValueFinder<K,V> implements IValueFinder<K,V> {

	String sqlTemplate = null;
	JdbcConnector databaseConnector = null;
	WKTReader wktReader = null;
	
	public SpatialValueFinder(JdbcConnector jdbcConnector, String sql) {
		
		this.databaseConnector = jdbcConnector;
		this.sqlTemplate = sql;
		this.wktReader = new WKTReader();
	}
	
	@Override
	public Geometry find(HashMap<K,V> dataset, K k) {
		
//		System.out.println("VALUE RETRIEVAL QUERY: " + sqlTemplate);
		
		Object val = databaseConnector.findSingletonValue(sqlTemplate.replace("$id", k.toString()));
		
		try {	
			if (val instanceof PGgeometry) {			
				val = wktReader.read(val.toString().substring(val.toString().indexOf(";") + 1));
				dataset.put(k, (V)val);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return (Geometry)val;
	}

}
