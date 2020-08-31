package eu.smartdatalake.simsearch.ranking.randomaccess;

import java.util.HashMap;

import eu.smartdatalake.simsearch.IDataConnector;
import eu.smartdatalake.simsearch.IValueFinder;

/**
 * Updates the in-memory lookup with NUMERICAL values used in random access operations during rank aggregation.
 * This method inserts the numerical attribute value found for a given object identifier (primary key) in a data source collection (DBMS table of REST API data).
 * @param <K>  The key (identifier) of the object.
 * @param <V>  The numerical value of this object at a particular attribute.
 */
public class NumericalValueFinder<K,V> implements IValueFinder<K,V> {

	String queryTemplate = null;
	IDataConnector dataConnector = null;
	
	/**
	 * Constructor of this class
	 * @param dataConnector  Instance of a connector to data source (JDBC connection to a DBMS or HTTP connection to a REST API).
	 * @param query  The query template to execute, either in SQL (against a DBMS) or JSON (against a REST API).
	 */
	public NumericalValueFinder(IDataConnector dataConnector, String query) {
		
		this.dataConnector = dataConnector;
		this.queryTemplate = query;
	}
	
	@Override
	public V find(HashMap<K,V> dataset, K k) {

//		System.out.println("VALUE RETRIEVAL QUERY: " + sqlTemplate);
		// Replace identifier in the query template and execute
		V val = (V)dataConnector.findSingletonValue(queryTemplate.replace("$id", k.toString()));
		// Insert numerical value into the lookup
		dataset.put(k, val);

		return val;		
	}

}