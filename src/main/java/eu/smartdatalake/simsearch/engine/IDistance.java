package eu.smartdatalake.simsearch.engine;

/**
 * Interface to various distance measures or metrics used in similarity calculations.
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
	
	/**
	 * Calculates the absolute difference between two distance values.
	 * @param a  The first distance value.
	 * @param b  The second distance value.
	 * @return  The absolute difference between the two distance values.
	 */
	public double diff(double a, double b);
	
	/**
	 * Specifies the default distance value in case one operand is a NaN-valued object (e.g., point, numerical attribute).
	 * @param d  The default distance value.
	 */
	public void setNaNdistance(double d);
}
