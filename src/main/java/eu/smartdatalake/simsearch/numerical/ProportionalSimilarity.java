package eu.smartdatalake.simsearch.numerical;

import eu.smartdatalake.simsearch.ISimilarity;

/**
 * Implements a numerical similarity measure based on difference of values proportional to the domain range.
 * @param <V>  Type variable to represent the values involved in the similarity calculations (usually, double values).
 */
public class ProportionalSimilarity<V> implements ISimilarity<V> {

	double baseElement;
	double domainKeyRange;
	
	/**
	 * Constructor #1
	 * @param baseElement  The numerical value specified in the search query.
	 * @param domainKeyRange   The range of the data domain.
	 */
	public ProportionalSimilarity(double baseElement, double domainKeyRange) {
		
		this.baseElement = baseElement;
		this.domainKeyRange = domainKeyRange;
	}
	
	/**
	 * Constructor #2
	 * @param domainKeyRange  The range of the data domain.
	 */
	public ProportionalSimilarity(double domainKeyRange) {

		this.domainKeyRange = domainKeyRange;
	}
	
	/**
	 * Returns the similarity score in [0..1] of the given (numerical) value against the fixed query value.
	 */
	@Override
	public double calc(V v) {
		if (v != null)
			return 1.0 - (Math.abs(this.baseElement - Double.parseDouble(v.toString())) / domainKeyRange);
		
		return 0.0;
	}
	

	/**
	 * Returns the similarity score in [0..1] between two NUMERICAL values.
	 * FIXED: Also returns adjusted similarities if values are beyond the range indexed in the B+-tree.
	 */
	@Override
	public double calc(V v1, V v2) {
		if ((v1 != null) && (v2 != null))
			return 1.0 - (Math.abs(Double.parseDouble(v1.toString())- Double.parseDouble(v2.toString())) / domainKeyRange); 

		return 0.0;
	}
}
