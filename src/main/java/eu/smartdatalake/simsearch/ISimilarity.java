package eu.smartdatalake.simsearch;

/**
 * Interface to various similarity measures
 * @param <V>  Type variable to represent the values involved in the similarity calculations (usually, double values).
 */
public interface ISimilarity<V> {

	/**
	 * Calculates similarity scores against a fixed query value according to different measures (e.g., Jaccard, exponential decay).
	 * The data types of the compared values must be the same or comparable.
	 * @param v  The data value to compare with a fixed query value.
	 * @return  Estimated similarity score in [0..1].
	 */
	public double calc(V v);
	
	/**
	 * Calculates similarity scores between two values according to different measures (e.g., Jaccard, exponential decay).
	 * The data types of the compared values must be the same or comparable.
	 * @param v1  The first value.
	 * @param v2  The second value.
	 * @return  Estimated similarity score in [0..1].
	 */
	public double calc(V v1, V v2);
	
}
