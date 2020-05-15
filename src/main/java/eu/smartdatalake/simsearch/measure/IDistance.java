package eu.smartdatalake.simsearch.measure;

/**
 * Interface to various similarity distances
 * @param <V>  Type variable to represent the distance values involved in the similarity calculations (usually, double values).
 */
public interface IDistance<V> {
	
	/**
	 * Calculates distance from a fixed query value according to different measures (e.g., Jaccard, Euclidean).
	 * The data types of the compared values must be the same or comparable.
	 * @param v  The data value to compare with a fixed query value.
	 * @return  Estimated distance value.
	 */
	public double calc(V v);
	
	/**
	 * Calculates the distance between two values according to different measures (e.g., Jaccard, Euclidean).
	 * The data types of the compared values must be the same or comparable.
	 * @param v1  The first value.
	 * @param v2  The second value.
	 * @return  Estimated distance value.
	 */
	public double calc(V v1, V v2);
	
}
