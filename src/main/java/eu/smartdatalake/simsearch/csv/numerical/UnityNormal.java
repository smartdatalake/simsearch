package eu.smartdatalake.simsearch.csv.numerical;

/**
 * Unity-based normalization (a.k.a. feature scaling) to bring each given value into the range [0,1]
 * @param <V>  Type variable to represent the values involved in the similarity calculations (usually, double values).
 */
public class UnityNormal<V> implements INormal<V> {

	double avgVal, minVal, maxVal;
	
	/**
	 * Constructor
	 * @param avgVal  The average of the data values (e.g., a numerical attribute in a dataset). 
	 * @param minVal  The minimum of the data values.
	 * @param maxVal  the maximum of the data values.
	 */
	public UnityNormal (double avgVal, double minVal, double maxVal) {
		
		this.avgVal = avgVal;
		this.minVal = minVal;
		this.maxVal = maxVal;
	}
	
	@Override
	public double normalize(V v) {
		
		return (Double.parseDouble(v.toString()) - avgVal) / (maxVal - minVal);
	}
}
