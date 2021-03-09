package eu.smartdatalake.simsearch.ranking;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import eu.smartdatalake.simsearch.csv.numerical.INormal;
import eu.smartdatalake.simsearch.manager.DatasetIdentifier;
import eu.smartdatalake.simsearch.measure.ISimilarity;
import eu.smartdatalake.simsearch.pivoting.MetricSimilarity;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Point;

/**
 * A matrix containing the similarity between all possible combinations in the query results.
 */
public class ResultMatrix {

	// List of all data/index datasetIdentifiers involved in the search
	Map<String, DatasetIdentifier> datasetIdentifiers;
	
	// Weights
	Map<String, Double> weights;
	
	// Normalization functions
	Map<String, INormal> normalizations;
	
	//Dictionaries built for the various datasets as needed in random access similarity calculations
	// Using the column number as a reference to the collected values for each attribute
	Map<String, Map<?, ?>> datasets;

	// List of similarity functions to be used in random access calculations
	Map<String, ISimilarity> similarities;
	
	// List of metric similarities per attribute (to be applied in pivot-based search only)
	Map<String, MetricSimilarity> metricSimilarities;
	
	/**
	 * Constructor
	 * @param datasetIdentifiers List of the attributes involved in similarity search queries
	 * @param lookups   List of the various data collections involved in the similarity search queries.
	 * @param similarities   List of the similarity measures applied in each search query.
	 * @param weights  List of the weights to be applied in similarity scores returned by each search query.
	 * @param normalizations  List of normalization functions to be applied in data values during random access.
	 * @param metricSimilarities  List of metric similarities per attribute (to be applied in pivot-based search only).
	 */
	public ResultMatrix(Map<String, DatasetIdentifier> datasetIdentifiers, Map<String, Map<?, ?>> lookups, Map<String, ISimilarity> similarities, Map<String, Double> weights, Map<String, INormal> normalizations, Map<String, MetricSimilarity> metricSimilarities) {
		
		this.datasetIdentifiers = datasetIdentifiers;
		this.datasets = lookups;
		this.similarities = similarities;
		this.weights = weights;
		this.normalizations = normalizations;
		this.metricSimilarities = metricSimilarities;
	}


	/**
	 * Post-processing of the search results in order to calculate their pairwise similarity.
	 * @param results  Array of results returned after multi-faceted similarity search.
	 * @return  A similarity matrix with result datasetIdentifiers and the estimated similarity scores per pair.
	 */
	public ResultPair[] calc(RankedResult[] results) {
		
		// No ranked results, so no matrix can be produced
		if ((results == null) || (results.length == 0))
			return null;   // Empty array

		// Full symmetrical similarity matrix with all pairs;  NOT USED: keeping only distinct combinations
		ResultPair[] matrix = new ResultPair[results.length * results.length];  // IntMath.factorial(results.length)/(2 * IntMath.factorial(results.length-2))
		
		// Keep correspondence of column names to datasetIdentifiers (hash keys) used in parameterization on weights, normalizations, and similarity measures
		// FIXME: Assuming the each attribute (column) is uniquely identified by its name and operation (even if multiple datasets are involved in the search)
		Map<String, String> colIdentifiers = new HashMap<String, String>();
		for (int r = 0; r < results[0].attributes.length; r++) {			
			for (Map.Entry<String, DatasetIdentifier> entry : this.datasetIdentifiers.entrySet()) {
				if (entry.getValue().getValueAttribute().equals(results[0].attributes[r].getName())) {
					// Attribute is used in pivot-based search
					if ((this.metricSimilarities != null) && (this.metricSimilarities.get(entry.getValue().getHashKey()) != null)) {
						colIdentifiers.put(results[0].attributes[r].getName(), entry.getValue().getHashKey());
						break;
					}
					// Attribute is involved in rank aggregation
					if ((this.similarities != null) && (this.similarities.get(entry.getValue().getHashKey()) != null)) {
						colIdentifiers.put(results[0].attributes[r].getName(), entry.getValue().getHashKey());
						break;
					}
				}
			}
		}
		
		RankedResult left, right;
		ResultPair pair;
			
		// The sum of weights specified for the given batch
		double sumWeights = this.weights.values().stream().mapToDouble(f -> f.doubleValue()).sum();
		
		// Full similarity matrix: Calculate pairwise similarity scores for all results
		for (int i = 0; i < results.length; i++) {
			left = results[i];
			for (int j = 0; j < results.length; j++) {
				right = results[j];
				pair = new ResultPair(left.getId(), right.getId());
				double score = 0.0;
				// Estimate similarity score according to the specifications for each attribute involved in the search
				for (int r = 0; r < right.attributes.length; r++) {
					// Identify the attribute values to compare for this pair
					String hashKey = colIdentifiers.get(right.attributes[r].getName());
					Object valLeft = this.datasets.get(hashKey).get(left.getId());
					Object valRight = this.datasets.get(hashKey).get(right.getId());
					if ((valLeft != null) && (valRight != null)) {   // If not null, then update score accordingly
						if ((this.metricSimilarities != null) && (this.metricSimilarities.get(hashKey) != null))    // The respective distance metric in pivot-based search; CAUTION! weights identified using attribute names
							score += this.weights.get(right.attributes[r].getName()) * this.metricSimilarities.get(hashKey).calc((Point) valLeft, (Point) valRight);
						else if (this.normalizations.get(hashKey) != null)   // Apply normalization, if specified for ranked aggregation
							score += this.weights.get(hashKey) * this.similarities.get(hashKey).calc(this.normalizations.get(hashKey).normalize(valLeft), this.normalizations.get(hashKey).normalize(valRight));
						else   // No normalization in ranked aggregation methods
							score += this.weights.get(hashKey) * this.similarities.get(hashKey).calc(valLeft, valRight);
					}				
				}
				pair.setScore(score / sumWeights);   // Weighted aggregate score over all attribute values
				matrix[i*results.length + j] = pair;
			}
		}		
		return matrix;
	}
	
}
