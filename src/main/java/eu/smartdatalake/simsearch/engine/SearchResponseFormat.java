package eu.smartdatalake.simsearch.engine;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import eu.smartdatalake.simsearch.Assistant;
import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.csv.numerical.INormal;
import eu.smartdatalake.simsearch.manager.DatasetIdentifier;
import eu.smartdatalake.simsearch.measure.ISimilarity;
import eu.smartdatalake.simsearch.pivoting.MetricSimilarity;
import eu.smartdatalake.simsearch.ranking.RankedResult;
import eu.smartdatalake.simsearch.ranking.ResultMatrix;
import eu.smartdatalake.simsearch.ranking.ResultPair;

/**
 * Formatting the response to a multi-attribute similarity search query to be reported back as a single object (typically in JSON). 
 * Adds notifications, associates entity names to each result (if applicable), and invokes calculation of similarity matrices per weight combination.
 */
public class SearchResponseFormat {

	Assistant myAssistant;
	
	/**
	 * Constructor
	 */
	public SearchResponseFormat() {
		
		myAssistant = new Assistant();
	}
	
	
	/**
	 * Formats the response to the multi-attribute similarity search query.
	 * @param results  An array of final top-k ranked results (i.e., qualifying entities) per weight combination.
	 * @param weights  Dictionary of the (possibly multiple alternative) weights per attribute to be applied in scoring the final results. 
	 * @param datasetIdentifiers  Dictionary of the attributes available for similarity search operations.
	 * @param datasets  Dictionary of the attribute datasets available for querying.
	 * @param lookups  Dictionary of the various data collections involved in the similarity search queries.
	 * @param similarities  Dictionary of the similarity measures applied in each search query.
	 * @param normalizations  Dictionary of normalizations applicable to numerical attribute data. 
	 * @param metricSimilarities  Dictionary of distance similarity metrics (one per queried attribute) to be used in pivot-based search.
	 * @param collectQueryStats  Boolean flag to indicate whether to produce the similarity matrix between all pairs of results.
	 * @param topk  The count of ranked aggregated results to collect, i.e., those with the top-k (highest) aggregated similarity scores.
	 * @param outCSVWriter  Writes to output CSV file (if applicable).
	 * @return  An array of formatted responses (one per weight combination) to be sent back to the user. 
	 */
	public SearchResponse[] proc(IResult[][] results, Map<String, Double[]> weights, Map<String, DatasetIdentifier> datasetIdentifiers, Map<String, Map<?, ?>> datasets, Map<String, Map<?, ?>> lookups, Map<String, ISimilarity> similarities, Map<String, INormal> normalizations, Map<String, MetricSimilarity> metricSimilarities, int topk, boolean collectQueryStats, OutputWriter outCSVWriter) {
		
		// Number of combinations of weights that have been applied
		int numWeights = weights.entrySet().iterator().next().getValue().length;
		SearchResponse[] responses = new SearchResponse[numWeights];
			
		// Produce final results for each combination of weights
		for (int w = 0; w < numWeights; w++) {
			Map<String, Double> curWeights = new HashMap<String, Double>();
			Iterator it = weights.entrySet().iterator();
		    while (it.hasNext()) {
		    	Map.Entry pair = (Map.Entry)it.next();
		    	curWeights.put((String) pair.getKey(), ((Double[])pair.getValue())[w]);
		    }		    
	
			// Prepare a response to this combination of weights
			SearchResponse response = new SearchResponse();
			response.setWeights(curWeights.values().toArray(new Double[0]));
			
			// OPTIONAL: Use names and URL identifiers for the final results
			Map<?,?> luNames = null;  	// An optional (non-queryable) dataset that contains names of entities
			String prefixURL = null; 	// A prefix to be used in entity identifiers for the results
			// No need to compute similarity matrix in evaluation tests 
			if (!collectQueryStats) {
				for (Map.Entry<String, DatasetIdentifier> entry : datasetIdentifiers.entrySet())  {
					// Use the dictionary containing names
					if ((!entry.getValue().isQueryable()) && (entry.getValue().isUsedForNames())) {
						luNames = datasets.get(entry.getKey());
						prefixURL = datasetIdentifiers.get(entry.getKey()).getPrefixURL();
					}		
				}
	
				// Post-processing of the search results in order to calculate their pairwise similarity
				// Cost to calculate the result similarity matrix grows quadratically with increasing k; if k > 50 results this step is skipped from the answer
				if (results[w].length <= Constants.K_MAX) {   
					ResultMatrix matrixCalculator = new ResultMatrix(datasetIdentifiers, lookups, similarities, curWeights, normalizations, metricSimilarities);
					// FIXME: IResult array is cast to RankedResult array in order to calculate the matrix
					RankedResult[] rankedResults = new RankedResult[results[w].length];
					System.arraycopy(results[w], 0, rankedResults, 0, results[w].length);		
					ResultPair[] simMatrix = matrixCalculator.calc(rankedResults);
					// Some manipulation in order to return URL identifiers in the similarity matrix
					if ((simMatrix != null) && (prefixURL != null)) {
						for (int i = 0; i < simMatrix.length; i++) { 					
							simMatrix[i].setLeft(myAssistant.formatURL(prefixURL, simMatrix[i].getLeft()));
							simMatrix[i].setRight(myAssistant.formatURL(prefixURL, simMatrix[i].getRight()));
						}
					}					
					response.setSimilarityMatrix(simMatrix);
				}			
			}
			// Populate overall response to this multi-attribute search query
			response.setRankedResults(results[w]);
	
			// Extra manipulation to replace identifiers with their respective URLs in the search results
			// Also include entity names, if available
			if ((luNames != null) && (prefixURL != null) && (results[w].length <= 1000)) {
				IResult[] curResults = response.getRankedResults();
				for (int i = 0; i < curResults.length; i++) { 
					String name = "";    // Default name
					// Names are actually kept in a dictionary
					if (luNames.get(curResults[i].getId()) != null)
						name = luNames.get(curResults[i].getId()).toString();
					curResults[i].setName(name);

					// Set the URL as the identifier to be reported
					curResults[i].setId(myAssistant.formatURL(prefixURL, curResults[i].getId()));	
				}
			}
				
			// Notifications
			if (results[w].length < topk)
				response.setNotification("Search inconclusive because at least one query facet failed to provide a sufficient number of candidates.");
			else
				response.setNotification("");
			
			responses[w] = response;	
			
			// Print results to a CSV file (if applicable)
			if (outCSVWriter.isSet()) {
				outCSVWriter.writeResults(curWeights.values().toArray(new Double[0]), results[w]);
			}	    
		}
				
		return responses;
	}
	
}
