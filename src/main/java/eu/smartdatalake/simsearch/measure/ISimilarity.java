package eu.smartdatalake.simsearch.measure;

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
	
	/**
	 * Provides the similarity score that corresponds to the given distance value.
	 * @param distance  A numeric value representing the distance of the resulting item from the query value.
	 * @return  A similarity score in [0..1].
	 */
	public double scoring(double distance);
	
	/**
	 * Provides a handle to the distance measure applied: Jaccard for categorical search; numerical difference for numbers; Haversine for spatial locations.
	 * @return  An instantiation of the distance measure 
	 */
	public IDistance<V> getDistanceMeasure();
	
	/**
	 * Indicates whether the scaling factor has been set in order to report similarity scores.
	 * @return  True, if the scale factor is set (i.e., a value above 0); otherwise, False.
	 */
	public boolean isScaleSet();

	/**
	 * Provides the serial number of the task (thread) involved in the similarity search.
	 * @return  An integer value that is used to identify the thread.
	 */
	public int getTaskId();
	
}
