package eu.smartdatalake.simsearch.categorical;

import java.util.HashSet;
import java.util.Set;

import eu.smartdatalake.simsearch.ISimilarity;

/**
 * Implements a categorical (textual) similarity measure.
 * @param <V>  Type variable to represent the values involved in the similarity calculations (usually, sets of tokens).
 */
public class CategoricalSimilarity<V> implements ISimilarity<V> {

	Set<String> baseTokens;

	/**
	 * Constructor
	 * @param querySet  The set of tokens (e.g., keywords) specified in the search query.
	 */
	public CategoricalSimilarity(TokenSet querySet) {
		
		this.baseTokens = new HashSet<String>(querySet.tokens);
	}

	
	/**
	 * Returns the similarity score in [0..1] of the given set of tokens against the fixed query set.
	 * FIXME: Calculates Jaccard similarity by default; Include other similarity measures?
	 */
	@Override
	public double calc(V v) {
		
		TokenSet curSet = (TokenSet) v;
		Set<String> union = new HashSet<String>(baseTokens);
		union.addAll(curSet.tokens);

		Set<String> intersection = new HashSet<String>(baseTokens);
		intersection.retainAll(curSet.tokens);
		
		return ((double) intersection.size()) / ((double) union.size());

	}

	/**
	 * Returns the similarity score in [0..1] between two sets of tokens.
	 * FIXME: Calculates Jaccard similarity by default; Include other similarity measures?
	 */
	@Override
	public double calc(V v1, V v2) {

		TokenSet set1 = (TokenSet) v1;
		TokenSet set2 = (TokenSet) v2;
		Set<String> union = new HashSet<String>(set1.tokens);
		union.addAll(set2.tokens);

		Set<String> intersection = new HashSet<String>(set1.tokens);
		intersection.retainAll(set2.tokens);
		
		return ((double) intersection.size()) / ((double) union.size());
	}

}
