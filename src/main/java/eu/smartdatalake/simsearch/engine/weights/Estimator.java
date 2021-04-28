package eu.smartdatalake.simsearch.engine.weights;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

/**
 * Estimates a data-driven weight per queryable attribute if not specified by the user. 
 */
public class Estimator {

	// Collection of similarity scores of candidate entities per attribute
	private Map<String, List<Double>> attrScores;
	
	// Retains the estimated weights per attribute based on analysis of the respective scores 
	private Map<String, Double> estWeights;
	
	// The total of all estimated weights (even for attributes having user-specified weights); used for normalization
	private double sumWeights = 0.0;
	
	
	/**
	 * Constructor
	 */
	public Estimator() {
		
		attrScores = new HashMap<String, List<Double>>();
		estWeights = new HashMap<String, Double>();	
	}

	
	/**
	 * Specifies the input data (scores) to be analyzed for estimating weight for a given attribute.
	 * @param task  The identifier of the task (priority queue collecting candidate entities most similar in the given attribute).
	 * @param scores  A list of the scores for all candidate entities.
	 */
	public void setInput(String task, List<Double> scores) {
		
		attrScores.put(task, scores);
/*
		// Optionally, calculate various statistics over the numerical values
		Map<String, Double> stats = getStatistics(task);
		System.out.print(task);
		for (Map.Entry<String, Double> entry : stats.entrySet())
			System.out.print(" " + entry.getKey() + ": " + entry.getValue());
		System.out.println();
*/		
	}
	
	
	/**
	 * Calculate statistics over the scores of the candidate entities collected on the specified attribute (priority queue)
	 * @param task  The identifier of the priority queue collecting candidates on a queryable attribute.
	 * @return  The collection of statistics (min, max, avg, count) over the collected similarity scores.
	 */
	public Map<String, Double> getStatistics(String task) {
		
		Map<String, Double> stats = new HashMap<String, Double>();
		
		Double max = attrScores.get(task)
			      .stream()
			      .mapToDouble(d -> d)
			      .max().orElseThrow(NoSuchElementException::new);
		stats.put("max", max);
		
		Double min = attrScores.get(task)
			      .stream()
			      .mapToDouble(d -> d)
			      .min().orElseThrow(NoSuchElementException::new);
		stats.put("min", min);
		
		Double avg = attrScores.get(task)
			      .stream()
			      .mapToDouble(d -> d)
			      .average().orElseThrow(NoSuchElementException::new);
		stats.put("avg", avg);
		
		Double count = (double) attrScores.get(task).size();
		stats.put("count", count);
		
		return stats;
	}
	
	
	/**
	 * Validates whether internal structure can be used for weight estimation.
	 * @return  True, if weight estimation can be applied; otherwise, False.
	 */
	public boolean validate() {
		
		// At least one attribute should have missing weight to trigger estimation
		if (!estWeights.isEmpty())
			return true;
		
		return false;
	}
	
	
	/**
	 * Specifies that a weight is missing for the given task (representing an attribute) and must be estimated.
	 * @param task  The task identifier of the priority queue used for a queryable attribute.
	 */
	public void setMissingWeight(String task) {
		
		estWeights.put(task, null);
	}
	
	
	/**
	 * Indicates whether weight(s) are missing for the given attribute and must be estimated.
	 * @param task  The identifier of the task (priority queue collecting candidate entities most similar in the given attribute).
	 * @return  True, if no weight was specified for this attribute; otherwise, False.
	 */
	public boolean hasMissingWeight(String task) {
		
		if (estWeights.containsKey(task))
			return true;
		
		return false;
	}
	
	
	/**
	 * Auxiliary method that sets all estimated weights to 1.0.
	 * CAUTION: The assigned value must be subsequently normalized (done in the getWeights method).
	 */
	public void test() {
		for (String task : estWeights.keySet()) {
			sumWeights = 1.0;  
			estWeights.replace(task, 1.0);
		}
	}
	
	
	/**
	 * Estimates a weight per requested attribute.
	 * Simplified estimation based on standard deviations of the input values (scores).
	 */
	public void proc() {
		
		// Employ standard deviation as an indicator of the spread in similarity scores
		StandardDeviation sd = new StandardDeviation();
		// Estimate the standard deviation based on the scores obtained per priority queue (i.e., per attribute)
		for (String task : attrScores.keySet()) {
			double weight = sd.evaluate(attrScores.get(task).stream().mapToDouble(d -> d).toArray());
//			System.out.println("Standard deviation for " + task + " -> " + weight);
			sumWeights += weight;   // Sum-up estimated weights
			// Store this preliminary estimate (ONLY for attributes with missing weight), before normalization
			estWeights.replace(task, weight);
		}
		
		// OPTIONAL: The assigned value can be subsequently normalized
		for (String task : estWeights.keySet()) {
			// Alternative options for final weights
			// ALTERNATIVE (NOT USED): Estimate based on exponential decay is assigned to all combinations
//			double w = Math.exp(- estWeights.get(task)/sumWeights);
			// FIXME: Estimate gets normalized by the sum of all standard deviations
			double w = estWeights.get(task)/sumWeights;
			// ALTERNATIVE (NOT USED): The original standard deviation becomes the weight for this attribute
//			double w = estWeights.get(task);
			estWeights.replace(task, w);
		}		
		
	}
	
	/**
	 * Estimates a weight per requested attribute.
	 * Assigned weight is the p-th percentile of the input values (scores per attribute).
	 * @param k  The k-th item in the input ordered by descending score.
	 */
	public void proc(int k) {

		double p;  //The p-th percentile of the designated values in the input values
		// Employ percentile as an indicator of the spread in similarity scores
		Percentile perc = new Percentile();
		for (String task : attrScores.keySet()) {
			// The p-th percentile value depends on the top-k query parameter and should be close to 100
			p = 100.0 - ((100.0 * k) / attrScores.get(task).size());
//			System.out.println("Percentile: " + p);
			double weight = perc.evaluate(attrScores.get(task).stream().mapToDouble(Double::doubleValue).toArray(), p);
			estWeights.replace(task, weight);   // No normalization applied in these estimates
		}	
	}
	
	
	/**
	 * Sums up the estimated weights for all attributes with no user-specified weights.
	 * CAUTION! Any attributes with user-specified weights are excluded from estimation.
	 * @return  The sum of all estimated weights.
	 */
	private double getSumWeights() {
		
//		return estWeights.values().stream().mapToDouble(a -> a).sum();
		return sumWeights;
	}
	
	
	/**
	 * Provides the attributes where weight estimation will be applied.
	 * @return  A collection of attribute names.
	 */
	public List<String> getAttributesWithoutWeight() {
		return new ArrayList<String>(estWeights.keySet());
	}
	
	
	/**
	 * Provides the final estimated weights per attribute; only applied for attributes with missing weight(s).
	 * @param numCombinations  The number of weight combinations per attribute.
	 * @return  Estimated weights per attributes to be applied in issuing the query results.
	 */
	public Map<String, Double[]> getWeights(int numCombinations) {
		
		Map<String, Double[]> weights = new HashMap<String, Double[]>();		
//		double totalEstWeights = calcSumWeights();
		
		for (String task : estWeights.keySet()) {
			Double[] attrWeights = new Double[numCombinations];
			Arrays.fill(attrWeights, estWeights.get(task));
			weights.put(task, attrWeights);
		}
		
		return weights;
	}
	
	
	/**
	 * Provides the estimated weight for the given task identifier (i.e., this corresponds to a queryable attribute).
	 * @param task  The identifier of the task (priority queue collecting candidate entities most similar in the given attribute).
	 * @return  A double value representing the estimated weight.
	 */
	public double getWeight(String task) {
		
		return estWeights.get(task);
	}
	
}
