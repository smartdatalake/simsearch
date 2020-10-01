package eu.smartdatalake.simsearch.measure;

import java.util.HashSet;
import java.util.Set;

import eu.smartdatalake.simsearch.csv.categorical.TokenSet;

/**
 * Implements a categorical (textual) distance measure.
 * @param <V>  Type variable to represent the values involved in the distance calculations (usually, sets of tokens).
 */
public class CategoricalDistance<V> implements IDistance<V> {

	Set<String> baseTokens;

	/**
	 * Constructor
	 * @param querySet  The set of tokens (e.g., keywords) specified in the search query.
	 */
	public CategoricalDistance(TokenSet querySet) {
		
		this.baseTokens = new HashSet<String>(querySet.tokens);
	}

	
	/**
	 * Returns the distance of the given set of tokens from the fixed query set.
	 * Calculates Jaccard distance by default. FIXME: Include other similarity measures?
	 */
	@Override
	public double calc(V v) {
		
		TokenSet curSet = (TokenSet) v;
		
		Set<String> union = new HashSet<String>(baseTokens);
		union.addAll(curSet.tokens);

		Set<String> intersection = new HashSet<String>(baseTokens);
		intersection.retainAll(curSet.tokens);

//		System.out.println("Categorical value:" + v.toString() + " Unscaled distance:" +  (1.0 - (((double) intersection.size()) / ((double) union.size()))));

		// Jaccard distance value
		return (1.0 - (((double) intersection.size()) / ((double) union.size())));
	}

	/**
	 * Returns the distance between two sets of tokens.
	 * Calculates Jaccard distance by default. FIXME: Include other similarity measures?
	 */
	@Override
	public double calc(V v1, V v2) {
	
		TokenSet set1 = (TokenSet) v1;
		TokenSet set2 = (TokenSet) v2;
		
		Set<String> union = new HashSet<String>(set1.tokens);
		union.addAll(set2.tokens);

		Set<String> intersection = new HashSet<String>(set1.tokens);
		intersection.retainAll(set2.tokens);

//		System.out.println("Categorical value 1:" + v1.toString() + "Categorical value 2:" + v2.toString() + " Unscaled distance:" +  (1.0 - (((double) intersection.size()) / ((double) union.size()))));
	
		// Jaccard distance value
		return (1.0 - (((double) intersection.size()) / ((double) union.size())));
	}

}
