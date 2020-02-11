package eu.smartdatalake.simsearch.numerical;

/**
 * Interface to normalization functions
 * @param <V>  Type variable to represent the values involved in the similarity calculations (usually, double values).
 */
public interface INormal<V> {

	/**
	 * Normalizes the given value according to a normalization function.
	 * @param v   The original value to be normalized.
	 * @return  The normalized value.
	 */
	public double normalize(V v);

}
