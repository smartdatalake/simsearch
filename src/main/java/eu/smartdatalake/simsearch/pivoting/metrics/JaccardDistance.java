package eu.smartdatalake.simsearch.pivoting.metrics;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import eu.smartdatalake.simsearch.engine.IDistance;

/**
 * Implements calculation of Jaccard distance between two equi-sized sets of tokens (keywords).
 * This method calculates a metric and is employed in pivot-based similarity search.
 * @param <V>  Type variable to represent the values involved in distance calculations (usually, string arrays).
 */
public class JaccardDistance<V> implements IDistance<V> {

	double nanDistance;
	
	@Override
	public double calc(V v) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double calc(V v1, V v2) {
		
		// Cast each argument to an array of strings
		return calc((String[]) v1, (String[]) v2);
	}
	
	@Override
	// Measure the absolute difference between two Jaccard distances
	public double diff(double a, double b) {
		return Math.abs(a - b);
	}


	/**
	 * Calculates the Jaccard distance between two equi-sized sets of keywords (tokens).
	 * @param tokens1  An array of string values representing the tokens of an entity.
	 * @param tokens2  An array of string values representing the token of another entity.
	 * @return  A double value measuring the Jaccard distance between the two entities based on the similarity in their tokens.
	 */
	private double calc(String[] tokens1, String[] tokens2) {
		
		Set<String> union = new HashSet<String>(Arrays.asList(tokens1));
		union.addAll(Arrays.asList(tokens2));

		Set<String> intersection = new HashSet<String>(Arrays.asList(tokens1));
		intersection.retainAll(Arrays.asList(tokens2));

		// Jaccard distance value
		return (1.0 - (((double) intersection.size()) / ((double) union.size())));
	}
	
	@Override
	public void setNaNdistance(double d) {
		
		nanDistance = d;
	}
	
}
