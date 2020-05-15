package eu.smartdatalake.simsearch.jdbc;

import java.util.HashMap;

import eu.smartdatalake.simsearch.IValueFinder;

/**
 * Updates the in-memory lookup with NUMERICAL values used in random access operations during rank aggregation.
 * This method inserts the numerical attribute value found for a given object identifier (primary key) in a database table.  
 * @param <K>  The key (identifier) of the object.
 * @param <V>  The numerical value of this object at a particular attribute.
 */
public class NumericalValueFinder<K,V> implements IValueFinder<K,V> {

	String sqlTemplate = null;
	JdbcConnector databaseConnector = null;
	
	public NumericalValueFinder(JdbcConnector jdbcConnector, String sql) {
		
		this.databaseConnector = jdbcConnector;
		this.sqlTemplate = sql;
	}
	
	@Override
	public V find(HashMap<K,V> dataset, K k) {

//		System.out.println("VALUE RETRIEVAL QUERY: " + sqlTemplate);
		
		V val = (V)databaseConnector.findSingletonValue(sqlTemplate.replace("$id", k.toString()));
		dataset.put(k, val);
	
		return val;		
	}

}
