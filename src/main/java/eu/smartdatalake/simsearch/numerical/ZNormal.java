package eu.smartdatalake.simsearch.numerical;

/**
 * Normalization using the z-score of the given value against a data distribution.
 * @param <V>  Type variable to represent the values involved in the similarity calculations (usually, double values).
 */
public class ZNormal<V> implements INormal<V> {

	double avgVal, stDev;
	
	/**
	 * Constructor
	 * @param avgVal  The average of the data values (e.g., a numerical attribute in a dataset). 
	 * @param stDev  The standard deviation of the data values.
	 */
	public ZNormal (double avgVal, double stDev) {
		
		this.avgVal = avgVal;
		this.stDev = stDev;
	}
	
	@Override
	public double normalize(V v) {

		return (Double.parseDouble(v.toString()) - avgVal) / stDev ;
	}

}
