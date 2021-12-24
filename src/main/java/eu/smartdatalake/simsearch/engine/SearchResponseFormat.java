package eu.smartdatalake.simsearch.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import eu.smartdatalake.simsearch.Assistant;
import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.engine.measure.ISimilarity;
import eu.smartdatalake.simsearch.engine.processor.RankedResult;
import eu.smartdatalake.simsearch.engine.processor.ResultMatrix;
import eu.smartdatalake.simsearch.engine.processor.ResultPair;
import eu.smartdatalake.simsearch.engine.weights.WeightInfo;
import eu.smartdatalake.simsearch.manager.DataType;
import eu.smartdatalake.simsearch.manager.DatasetIdentifier;
import eu.smartdatalake.simsearch.manager.ingested.numerical.INormal;
import eu.smartdatalake.simsearch.pivoting.MetricSimilarity;

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
	 * @param extraColumns  Names of extra attributes (not involved in similarity criteria) to include in the output
	 * @param weights  Dictionary of the (possibly multiple alternative) weights per attribute to be applied in scoring the final results. 
	 * @param datasetIdentifiers  Dictionary of the attributes available for similarity search operations.
	 * @param datasets  Dictionary of the attribute datasets available for querying.
	 * @param lookups  Dictionary of the various data collections involved in the similarity search queries.
	 * @param similarities  Dictionary of the similarity measures applied in each search query.
	 * @param normalizations  Dictionary of normalizations applicable to numerical attribute data. 
	 * @param metricSimilarities  Dictionary of distance similarity metrics (one per queried attribute) to be used in pivot-based search.
	 * @param collectQueryStats  Boolean flag to indicate whether to produce the similarity matrix between all pairs of results.
	 * @param topk  The count of ranked aggregated results to collect, i.e., those with the top-k (highest) aggregated similarity scores.
	 * @param notification  Message notification to be returned in case of errors.
	 * @param execTime  Evaluation time (in seconds).
	 * @param outWriter  Writes results to an output CSV file or to standard output (if applicable).
	 * @return  An array of formatted responses (one per weight combination) to be sent back to the user. 
	 */
	public SearchResponse[] proc(IResult[][] results, String[] extraColumns, Map<String, Double[]> weights, Map<String, DatasetIdentifier> datasetIdentifiers, Map<String, Map<?, ?>> datasets, Map<String, Map<?, ?>> lookups, Map<String, ISimilarity> similarities, Map<String, INormal> normalizations, Map<String, MetricSimilarity> metricSimilarities, int topk, boolean collectQueryStats, String notification, double execTime, OutputWriter outWriter) {
		
		// Number of combinations of weights that have been applied
		int weightCombinations = weights.entrySet().iterator().next().getValue().length;
		SearchResponse[] responses = new SearchResponse[weightCombinations];

		// Produce final results for each combination of weights
		for (int w = 0; w < weightCombinations; w++) {
			// Get the weights per attribute applied in this combination
			Map<String, Double> curWeights = new HashMap<String, Double>();
			Map<String, Double> repWeights = new HashMap<String, Double>();
		    List<WeightInfo> resWeights = new ArrayList<WeightInfo>();   // Just for reporting in the response
		    for (Entry<String, Double[]> entry : weights.entrySet()) {
		    	// CAUTION: The original attribute name is used as key in pivot-based search; the task id (priority queue) is used in rank aggregation
		    	WeightInfo aWeight = new WeightInfo(((metricSimilarities != null) ? entry.getKey() : datasetIdentifiers.get(entry.getKey()).getValueAttribute()), (entry.getValue())[w]);	    	 
		    	resWeights.add(aWeight);
		    	curWeights.put(entry.getKey(), aWeight.getValue());
		    	repWeights.put(aWeight.getAttribute(), aWeight.getValue());
		    }
		    
			// Prepare a response for this combination of weights
			SearchResponse response = new SearchResponse();
			response.setWeights(resWeights.toArray(new WeightInfo[resWeights.size()]));
		
			int numApproxResults = 0;
			
			// OPTIONAL: Include extra attribute values and URL identifiers for the final results
			Map<String, Map<?,?>> extraLookups = new HashMap<String, Map<?,?>>();  	// Optional datasets regarding reporting extra attributes (not involved in similarity criteria)
			List<String> temporalAttrs = new ArrayList<String>();   // List of temporal attributes for value conversion
			String prefixURL = null; 	// A prefix to be used in entity identifiers for the results
			// No need to compute similarity matrix in evaluation tests 
			if (!collectQueryStats) {
				for (Map.Entry<String, DatasetIdentifier> entry : datasetIdentifiers.entrySet())  {
					// Use the dictionary containing names
					if ((!entry.getValue().isQueryable()) && (entry.getValue().isUsedForNames())) {
						prefixURL = datasetIdentifiers.get(entry.getKey()).getPrefixURL();
					}
					// Identify lookups for any extra columns required for the output
					if ((extraColumns != null) && (Arrays.stream(extraColumns).anyMatch(entry.getValue().getValueAttribute()::equals))) {
						if (datasets.get(entry.getKey()) != null)   	// Existing dataset ingested in memory
							extraLookups.put(entry.getValue().getValueAttribute(), datasets.get(entry.getKey()));
						else if (lookups.get(entry.getKey()) != null)	// Lookup constructed at query time
							extraLookups.put(entry.getValue().getValueAttribute(), lookups.get(entry.getKey()));
						else	// No lookup found
							continue;
						// Keep track of any temporal attributes in order to convert them to date/time values
						if (entry.getValue().getDatatype() == DataType.Type.DATE_TIME) 
							temporalAttrs.add(entry.getValue().getValueAttribute());
					}	
				}
				
				// Check if any user-specified columns were not found
				if (extraColumns != null) {
					List<String> invalidColumns = Arrays.stream(extraColumns)
					    .filter(el -> Arrays.stream(extraLookups.keySet().toArray()).noneMatch(el::equals))
					    .collect(Collectors.toList());
					if (!invalidColumns.isEmpty()) {
						String msg = "Attribute(s) " + Arrays.toString(invalidColumns.toArray(new String[0])) + " not found.";
						System.out.println("NOTICE: " + msg);
						notification.concat(msg);
					}
				}
				
				// Post-processing of the search results in order to calculate their pairwise similarity
				// Cost to calculate the result similarity matrix grows quadratically with increasing k; if k > 50 results this step is skipped from the answer
				if (results[w].length <= Constants.K_MAX) {   
					ResultMatrix matrixCalculator = new ResultMatrix(datasetIdentifiers, lookups, similarities, curWeights, normalizations, metricSimilarities);	
					ResultPair[] simMatrix = matrixCalculator.calc(results[w]);
					// Some manipulation in order to return URL identifiers in the similarity matrix
					if ((simMatrix != null) && (prefixURL != null)) {
						for (int i = 0; i < simMatrix.length; i++) { 					
							simMatrix[i].setLeft(myAssistant.formatURL(prefixURL, simMatrix[i].getLeft()));
							simMatrix[i].setRight(myAssistant.formatURL(prefixURL, simMatrix[i].getRight()));
						}
					}				
					response.setSimilarityMatrix(simMatrix);
				}	
			
				// Manipulation to report any extra attributes in the search results
				Object val;
				if ((extraLookups != null) && (results[w].length <= Constants.K_MAX)) {
					for (int i = 0; i < results[w].length; i++) { 
						// Add value for each extra column required for the output
						Map<String, Object> extraValues = new HashMap<String, Object>();
						for (String col: extraLookups.keySet()) {
							if ((val = extraLookups.get(col).get(results[w][i].getId())) != null) {
								if (temporalAttrs.contains(col))   // Convert to date/time value
									extraValues.put(col, myAssistant.formatDateValue(val));  
								else
									extraValues.put(col, myAssistant.formatAttrValue(val));
							}
							else
								extraValues.put(col, "");
						}
						((RankedResult)(results[w][i])).setExtraAttributes(extraValues);
						
						// Check if at least some results are approximate due to time out
						if (!results[w][i].isExact())
							numApproxResults++;
						
						// Set the URL as the identifier to be reported
						if (prefixURL != null)
							results[w][i].setId(myAssistant.formatURL(prefixURL, results[w][i].getId()));	
					}
				}
			}
			
			// Populate overall response to this multi-attribute search query
			response.setRankedResults(results[w]);
				
			// Notifications
			response.setNotification(notification);
			if (results[w].length < topk)
				response.appendNotification("Search inconclusive because at least one query facet failed to provide a sufficient number of candidates.");
			
			if (numApproxResults == response.getRankedResults().length)
				response.appendNotification("All results have been ranked approximately because the query timed out.");
			else if (numApproxResults > 0)
				response.appendNotification("The last " + numApproxResults + " results have been ranked approximately because the query timed out.");
			
			// Execution cost for statistics; the same time cost concerns all weight combinations
			response.setTimeInSeconds(execTime);
			
			responses[w] = response;	
			
			// Print results to a CSV file, a TXT file or to standard output (if applicable)
			if (outWriter.isStandardOutput()) {   	// Tabular format to standard output	
				outWriter.printResults(repWeights, results[w], execTime);	
			}
			else if (outWriter.isSet()) {  			
				if (outWriter.outColumnDelimiter != null) 	// CSV file
					outWriter.writeResults(repWeights.values().toArray(new Double[0]), results[w]);
				else if (!outWriter.outJsonFile)			// Tabular format to TXT file
					outWriter.printResults(repWeights, results[w], execTime);
			}	    
		}
				
		return responses;
	}
	
}
