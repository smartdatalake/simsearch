package eu.smartdatalake.simsearch.engine.measure;

import eu.smartdatalake.simsearch.engine.IDistance;

/**
 * Implements a numerical distance measure based on difference of values.
 * @param <V>  Type variable to represent the values involved in the distance calculations (usually, double values).
 */
public class NumericalDistance<V> implements IDistance<V> {

	double baseElement;
	
	/**
	 * Constructor #1
	 * @param baseElement  The numerical value specified in the search query.
	 */
	public NumericalDistance(double baseElement) {
		
		this.baseElement = baseElement;
	}
	
	
	/**
	 * Returns the absolute distance of the given (numerical) value from the fixed query value.
	 */
	@Override
	public double calc(V v) {
		// No need to check again for NULL values; already handled by DecayedSimilarity class
		
//		System.out.println("Numerical value:" + v.toString() + " Unscaled distance:" +  (Math.abs(this.baseElement - Double.parseDouble(v.toString()))));
//		if (v != null)
			return (Math.abs(this.baseElement - Double.parseDouble(v.toString())));
	}
	

	/**
	 * Returns the absolute distance between two NUMERICAL values.
	 */
	@Override
	public double calc(V v1, V v2) {
		// No need to check again for NULL values; already handled by DecayedSimilarity class
//		if ((v1 != null) && (v2 != null))
		return (Math.abs(Double.parseDouble(v1.toString())- Double.parseDouble(v2.toString()))); 
	}
	
	@Override
	// Measure the absolute difference between two numerical distances
	public double diff(double a, double b) {
		return Math.abs(a - b);
	}


	@Override
	public void setNaNdistance(double d) {
		// TODO Auto-generated method stub
	}
	
}
