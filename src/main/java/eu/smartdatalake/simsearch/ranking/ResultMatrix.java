package eu.smartdatalake.simsearch.ranking;

import java.util.HashMap;
import java.util.Map;

import eu.smartdatalake.simsearch.DatasetIdentifier;
import eu.smartdatalake.simsearch.csv.numerical.INormal;
import eu.smartdatalake.simsearch.measure.ISimilarity;

public class ResultMatrix {

	// List of all data/index datasetIdentifiers involved in the search
	Map<String, DatasetIdentifier> datasetIdentifiers;
	
	// Weights
	Map<String, Double> weights;
	
	// Normalization functions
	Map<String, INormal> normalizations;
	
	//Dictionaries built for the various datasets as needed in random access similarity calculations
	// Using the column number as a reference to the collected values for each attribute
	Map<String, HashMap<?, ?>> datasets;

	// List of similarity functions to be used in random access calculations
	Map<String, ISimilarity> similarities;
	
	/**
	 * Constructor
	 * @param datasetIdentifiers List of the attributes involved in similarity search queries
	 * @param datasets   List of the various data collections involved in the similarity search queries.
	 * @param similarities   List of the similarity measures applied in each search query.
	 * @param weights  List of the weights to be applied in similarity scores returned by each search query.
	 * @param normalizations  List of normalization functions to be applied in data values during random access.
	 */
	public ResultMatrix(Map<String, DatasetIdentifier> datasetIdentifiers, Map<String, HashMap<?, ?>> datasets, Map<String, ISimilarity> similarities, Map<String, Double> weights, Map<String, INormal> normalizations) {
		
		this.datasetIdentifiers = datasetIdentifiers;
		this.datasets = datasets;
		this.similarities = similarities;
		this.weights = weights;
		this.normalizations = normalizations;
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

		// Full similarity matrix;  NOT USED: keeping only distinct combinations
		ResultPair[] matrix = new ResultPair[results.length * results.length];  // IntMath.factorial(results.length)/(2 * IntMath.factorial(results.length-2))
		
		// Keep correspondence of column names to datasetIdentifiers (hash keys) used in parameterization on weights, normalizations, and similarity measures
		// FIXME: Assuming the each attribute (column) is uniquely identified by its name (even if multiple datasets are involved in the search)
		Map<String, String> colIdentifiers = new HashMap<String, String>();
		for (int r = 0; r < results[0].attributes.length; r++) {			
			for (Map.Entry<String, DatasetIdentifier> entry : this.datasetIdentifiers.entrySet()) {
				if (entry.getValue().getColumnName().equals(results[0].attributes[r].getName())) {
					colIdentifiers.put(results[0].attributes[r].getName(), entry.getValue().getHashKey());
					break;
				}
			}
		}
		
		RankedResult left, right;
		ResultPair pair;
		
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
						if (this.normalizations.get(hashKey) != null)   // Apply normalization, if specified
							score += this.weights.get(hashKey) * this.similarities.get(hashKey).calc(this.normalizations.get(hashKey).normalize(valLeft), this.normalizations.get(hashKey).normalize(valRight));
						else
							score += this.weights.get(hashKey) * this.similarities.get(hashKey).calc(valLeft, valRight);
					}				
				}
				pair.setScore(score);
				matrix[i*results.length + j] = pair;			
			}
		}		
		return matrix;
	}
	
}
