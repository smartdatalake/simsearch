package eu.smartdatalake.simsearch.manager;

/**
 * Interface for connecting to data sources and querying them in-situ.
 */
public interface IDataConnector {

	/**
	 * Query that fetches only a single value (i.e. from the first result); Assuming that one value is only needed.
	 * @param query  The query to be submitted for fetching this value, e.g., an SQL command for the SELECT query.
	 * @return  An object representing an attribute value (e.g., the value at a specific attribute for a given object identifier).
	 */
	public Object findSingletonValue(String query);
	
}
