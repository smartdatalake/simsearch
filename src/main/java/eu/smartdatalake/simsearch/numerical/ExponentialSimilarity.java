package eu.smartdatalake.simsearch.numerical;

import eu.smartdatalake.simsearch.ISimilarity;

/**
 * Implements numerical similarity measure based on an exponential decay function.
 * @param <V>  Type variable to represent the values involved in the similarity calculations (usually, double values).
 */
public class ExponentialSimilarity<V> implements ISimilarity<V> {

	double baseElement;
	double domainKeyRange;
	
	// Default decay constant, if not configured by the user
	double lambda = 200; // 0.2; // 5; //1; //0.0001; //25.0;  
	
	/**
	 * Constructor #1
	 * @param baseElement  The numerical value specified in the search query.
	 * @param domainKeyRange  The range of the data domain.
	 * @param decay   The decay constant to be applied in similarity calculations.
	 */
	public ExponentialSimilarity(double baseElement, double domainKeyRange, double decay) {
		
		this.baseElement = baseElement;
		this.domainKeyRange = domainKeyRange;
		this.lambda = decay;
	}
	
	/**
	 * Constructor #2
	 * @param domainKeyRange  The range of the data domain.
	 * @param decay  The decay constant to be applied in similarity calculations.
	 */
	public ExponentialSimilarity(double domainKeyRange, double decay) {

		this.domainKeyRange = domainKeyRange;
		this.lambda = decay;
	}
	
	/**
	 * Returns the similarity score in [0..1] of the given (numerical) value against the fixed query value.
	 */
	@Override
	public double calc(V v) {
		if (v != null)
			return Math.exp(-(Math.abs(this.baseElement - Double.parseDouble(v.toString()))) * lambda);   
		
		return 0.0;    // Default score: No similarity
	}
	
	/**
	 * Returns the similarity score in [0..1] between two NUMERICAL values
	 */
	@Override
	public double calc(V v1, V v2) {
		if ((v1 != null) && (v2 != null))
			return Math.exp(-(Math.abs(Double.parseDouble(v1.toString())- Double.parseDouble(v2.toString()))) * lambda);
				
		return 0.0;  // Default score: No similarity
	}
}
