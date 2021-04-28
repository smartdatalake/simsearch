package eu.smartdatalake.simsearch.engine.processor;

import java.util.List;
//import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Interface to specifying various types of similarity search queries against a dataset.
 * @param <K>  Type variable representing the keys of the data objects.
 * @param <V>  Type variable representing the values of the data objects.
 */
public interface ISimSearch<K, V> {
	
	/**
	 * Progressively provides the next available result from the similarity search query.
	 * @return  The next available result.
	 */
	public List<V> getNextResult();
	
	/**
	 * Progressively collects the results of a similarity search query.
	 * @param results  A collection with the query results.
	 */
//	public void compute(ConcurrentLinkedQueue<PartialResult> results);
	
}
