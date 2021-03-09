package eu.smartdatalake.simsearch.ranking;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import eu.smartdatalake.simsearch.Assistant;
import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.engine.IResult;
import eu.smartdatalake.simsearch.engine.PartialResult;
import eu.smartdatalake.simsearch.manager.DatasetIdentifier;
import eu.smartdatalake.simsearch.measure.ISimilarity;

/**
 * Wrapper to handle similarity search requests involving a single attribute, where no rank aggregation is actually required.
 * This actually returns the top-k results from the priority queue as obtained by the individual kNN query executed against the data source (a single attribute).
 */
public class SingletonRanking  implements IRankAggregator {

	Logger log = null;
	Assistant myAssistant;
	String val;
	double score;
	int topk;       // Number of ranked aggregated results to collect
	
	// Collection of queues that collect results from each running task
	Map<String, ConcurrentLinkedQueue<PartialResult>> queues;
	Map<String, Thread> tasks;
	
	// Collection of all data/index datasetIdentifiers involved in the search
	Map<String, DatasetIdentifier> datasetIdentifiers;
	
	// Weights
	Map<String, Double[]> weights;
	int weightCombinations;
	
	// Collection of atomic booleans to control execution of the various threads
	Map<String, AtomicBoolean> runControl = new HashMap<String, AtomicBoolean>();

	// Collection of similarity functions to be used in random access calculations
	Map<String, ISimilarity> similarities;
	
	// Collection of the ranked results to be given as output per weight combination
	ResultCollection[] results;
	
	// Sum of weights per combination
	double[] sumWeights; 
	
	/**
	 * Constructor
	 * @param datasetIdentifiers List of the attributes involved in similarity search queries.
	 * @param similarities   Dictionary of the similarity measures applied in each search query.
	 * @param weights  Dictionary of the (possibly multiple alternative) weights per attribute to be applied in scoring the final results. 
	 * @param tasks  Collection of running threads; each one executes a query and it is associated with its respective queue that collects its results.
	 * @param queues  Collection of the queues collecting results from each search query.
	 * @param runControl  Collection of boolean values indicating the status of each thread.
	 * @param topk  The count of ranked aggregated results to collect, i.e., those with the top-k (highest) aggregated similarity scores.
	 * @param log  Handle to the log file for notifications and execution statistics.
	 */
	public SingletonRanking(Map<String, DatasetIdentifier> datasetIdentifiers, Map<String, ISimilarity> similarities, Map<String, Double[]> weights, Map<String, Thread> tasks, Map<String, ConcurrentLinkedQueue<PartialResult>> queues, Map<String, AtomicBoolean> runControl, int topk, Logger log) {
		
		myAssistant = new Assistant();
		this.log = log;
		this.datasetIdentifiers = datasetIdentifiers;

		this.similarities = similarities;
		this.tasks = tasks;
		this.queues = queues;
		this.weights = weights;
		this.runControl = runControl;
		this.topk = topk;
		
		// Number of combinations of weights to apply
		weightCombinations = weights.entrySet().iterator().next().getValue().length;

		// Array of collection of results; one collection (list) per combination of weights
		results = new ResultCollection[weightCombinations];
		
		// Initialize all array structures
		for (int w = 0; w < weightCombinations; w++) {
			results[w] = new ResultCollection();
		}
	}


	/**
	 * Inserts or updates the ranked aggregated results based on a result from the i-th queue
	 * @param taskKey   The hashKey of the task to be checked for its next result.
	 * @param n   The rank of the new result.
	 * @return  A Boolean value: True, if the ranked aggregated list has been updated; otherwise, False.
	 */
	private boolean updateRankedList(String taskKey, int n) {

		PartialResult cand = queues.get(taskKey).peek(); 
		if (cand != null) {
			// Create a new resulting item and report its rank and its original identifier...
			RankedResult res = new RankedResult(1);   // A single attribute will be reported
			res.setId((String)cand.getId());
			res.setRank(n);
			
			// ... also its original attribute value and its calculated similarity score
			// Assuming a SINGLE, NOT NULL attribute value   
			ResultFacet attr = new ResultFacet();
			attr.setName(this.datasetIdentifiers.get(taskKey).getValueAttribute());
			attr.setValue(myAssistant.formatAttrValue(cand.getValue()));
			// Also its individual similarity score
			attr.setScore(cand.getScore());
			res.attributes[0] = attr;
			//... and its overall aggregated score
			res.setScore(cand.getScore());   // Aggregate score is equal to that on the singleton attribute
		
			// Indicate whether this ranking should be considered exact or not
			res.setExact(true);
		
			for (int w = 0; w < weightCombinations; w++) {
				// Issue result to the output queue
				results[w].add(res);
			}
			return true;
		}

		return false;
	}


	
	/**
	 * Implements the processing logic of the threshold-based algorithm.
	 */
	@Override
	public IResult[][] proc() {
			
		boolean running = true;
		int n = 0;
		// This is the key of the single task fetching candidates
		String taskKey = tasks.keySet().stream().findFirst().get();
		
		try {
			// Number of ranked aggregated results so far
			int[] k = new int[weightCombinations];    // Each k is monitoring progress for each combination of weights
			Arrays.fill(k, 0);
			
			// Calculate the sum of weights for each combination
			sumWeights = new double[weightCombinations];
			Arrays.fill(sumWeights, 0.0);
			for (int w = 0; w < weightCombinations; w++) {
				sumWeights[w] += this.weights.get(taskKey)[w];
			}		

			boolean allScalesSet = false;
			long startTime = System.currentTimeMillis();
			
			while (running) {

				// Wait until the scale factor for similarity scores has been set 
				while (allScalesSet == false) {
					allScalesSet = true;
					TimeUnit.NANOSECONDS.sleep(100);
					// In case that the task is completed and no scale has been set, this means that all results are exact and no scale is needed
					allScalesSet = allScalesSet && (!runControl.get(taskKey).get() || this.similarities.get(taskKey).isScaleSet());
				}

				n++;  // Showing rank as 1,2,3,... instead of 0,1,2,...
				running = false;
	
				// Wait until the examined queue has been updated with its n-th result
				boolean stop = true;

				// Wait a while and then try again to fetch results from the queue	
				while (updateRankedList(taskKey, n) == false) {
					TimeUnit.NANOSECONDS.sleep(1);
					if (System.currentTimeMillis() - startTime > Constants.RANKING_MAX_TIME)
						break;
				}			
				
				// Stop if all results have been acquired
				stop = stop && (( n > topk ));			
				
				// Determine whether to continue polling results from queue
				// Once top-k ranked results are returned, stop the running task gracefully
				// If the process times out, then stop any further examination
				if (stop || (System.currentTimeMillis() - startTime > Constants.RANKING_MAX_TIME)) {
					runControl.get(taskKey).set(false);   //Stop each task
					running = false; 		// Quit the ranked aggregation process
				} 
				else {  // Otherwise, the task is still running, so continue fetching results				
					queues.get(taskKey).poll();    // Remove already processed result from this queue
					// Once a queue is exhausted, it is no longer considered and search continues with the remaining queues
					if (tasks.get(taskKey).isAlive() || !queues.get(taskKey).isEmpty()) {						
						running = true;	
					}
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		this.log.writeln("In total " + n + " results have been fetched from the queue.");		
	
		// Prepare array of final results
		IResult[][] allResults = new IResult[weightCombinations][topk];
		for (int w = 0; w < weightCombinations; w++) {
			allResults[w] = results[w].toArray();
		}	
		
		return allResults;
	}

}
