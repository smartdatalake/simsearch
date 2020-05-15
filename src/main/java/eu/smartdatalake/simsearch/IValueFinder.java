package eu.smartdatalake.simsearch;

import java.util.HashMap;

/**
 * Interface for retrieving attribute values for a given object identifier.
 * @param <K>  Type of the keys used for the identifiers.
 * @param <V>  Type of the attribute values associated with the identifiers.
 */
public interface IValueFinder<K,V> {

	/**
	 * Retrieve the attribute value available in the dataset for a given object identifier.
	 * @param dataset  The in-memory dataset to be searched.
	 * @param k   The key (identifier) of the object.
	 * @return  The value of this object at a particular attribute.
	 */
	public Object find(HashMap<K,V> dataset, K k);
		
}
