package eu.smartdatalake.simsearch.engine.processor.ranking;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.engine.IResult;
import eu.smartdatalake.simsearch.engine.measure.ISimilarity;
import eu.smartdatalake.simsearch.manager.DatasetIdentifier;
import eu.smartdatalake.simsearch.manager.ingested.numerical.INormal;

/**
 * Implementation of the No Random Access algorithm for rank aggregation.
 */
public class NoRandomAccessRanking<K,V> extends RankAggregator<K,V> {

	String item;

	// Priority queues of results based on DESCENDING scores
	AggregateScoreQueue[] mapLowerBounds; 
	AggregateScoreQueue[] mapUpperBounds;
	
	/**
	 * Constructor
	 * @param datasetIdentifiers List of the attributes involved in similarity search queries.
	 * @param lookups   Dictionary of the various data collections involved in the similarity search queries.
	 * @param similarities   Dictionary of the similarity measures applied in each search query.
	 * @param weights  Dictionary of the (possibly multiple alternative) weights per attribute to be applied in scoring the final results. 
	 * @param normalizations  Dictionary of normalization functions to be applied in data values during random access.
	 * @param tasks   The collection of running threads; each one executes a query and it is associated with its respective queue that collects its results.
	 * @param queues  The collection of ranked lists (priority queues) collecting results from each search query.
	 * @param runControl  The collection of boolean values indicating the status of each thread.
	 * @param topk  The count of ranked aggregated results to collect, i.e., those with the top-k (highest) aggregated similarity scores.
	 * @param log  Handle to the log file for notifications and execution statistics.
	 */
	public NoRandomAccessRanking(Map<String, DatasetIdentifier> datasetIdentifiers, Map<String, Map<K, V>> lookups, Map<String, ISimilarity> similarities, Map<String, Double[]> weights, Map<String, INormal> normalizations, Map<String, Thread> tasks, Map<String, RankedList> queues, Map<String, AtomicBoolean> runControl, int topk, Logger log) {
		
		super(datasetIdentifiers, lookups, similarities, weights, normalizations, tasks, queues, runControl, topk, log);

		// Instantiate priority queues, a pair of queues per combination of weights
		// The first queue is on DESCENDING lower bounds...
		mapLowerBounds = new AggregateScoreQueue[weightCombinations]; 
		//...and the second on DESCENDING upper bounds of aggregated results
		mapUpperBounds = new AggregateScoreQueue[weightCombinations];
	
		// Initialize all array structures
		for (int w = 0; w < weightCombinations; w++) {
			mapLowerBounds[w] = new AggregateScoreQueue(topk+1);
			mapUpperBounds[w] = new AggregateScoreQueue(topk+1);
			curResults[w] = new AggregateResultCollection();
		}
	}
	

	/**
	 * Inserts or updates the ranked aggregated results based on a result from a queue.
	 * @param taskKey   The hashKey of the task to be checked for its next result.
	 * @return  A Boolean value: True, if the ranked aggregated list has been updated; otherwise, False.
	 */
	private boolean updateRankedList(String taskKey) {

		if (!queues.get(taskKey).isEmpty()) {
			PartialResult res = queues.get(taskKey).poll(); 	// Result removed from queue!
			if (res != null) {
				// A common identifier must be used per result
				item = res.getId().toString();
				//CAUTION! Weighted score used in aggregation
				for (int w = 0; w < weightCombinations; w++) {
					score = this.weights.get(taskKey)[w] * res.getScore();
					aggResult = curResults[w].get(item);
					if (aggResult != null) { 	// UPDATE: Aggregate result already
												// exists in the ranked list
						mapLowerBounds[w].remove(aggResult.getLowerBound(), item);
						// Update its lower bound
						aggResult.setLowerBound(aggResult.getLowerBound() + score); 
						// Mark that this result has also been retrieved from this queue
						aggResult.setAppearance(this.similarities.get(taskKey).getTaskId()); 
						// If all results have been received, the upper bound
						// coincides with the lower bound
						if (aggResult.checkAppearance()) {						
							mapUpperBounds[w].remove(aggResult.getUpperBound(), item);
							aggResult.setUpperBound(aggResult.getLowerBound());					
						}
	
						curResults[w].put(item, aggResult);
						// Maintain updated bounds in the priority queues
						mapLowerBounds[w].put(aggResult.getLowerBound(), item);	
						mapUpperBounds[w].put(aggResult.getUpperBound(), item);
					} else { // INSERT new aggregate result to the ranked list...
						aggResult = new AggregateResult(item, numTasks, score, score);
						// Mark that this result has also been retrieved from this queue
						aggResult.setAppearance(this.similarities.get(taskKey).getTaskId()); 
						curResults[w].put(item, aggResult);
						// ...and put it to the priority queues holding lower and upper bounds
						mapLowerBounds[w].put(score, item); 
						mapUpperBounds[w].put(score, item);
					}
				}
				return true;
			}
		}

		return false;
	}

	/**
	 * Updates upper bounds of ranked aggregated results at current iteration
	 * @param w   The identifier of the weight combination to be applied on the scores.
	 */
	private void updateUpperBounds(int w) {
		
		double ub;
		// Once a new result has been obtained from all queries, adjust the
		// UPPER bounds in each aggregated result
		for (String item : curResults[w].keySet()) {
			aggResult = curResults[w].get(item);
			if (aggResult.checkAppearance()) { //Upper bound is already fixed for this aggregated result
				continue;      // No need to make any updates
			}
			
			// Remove previous upper bound from the priority queue
			mapUpperBounds[w].remove(aggResult.getUpperBound(), item); 
			// Initialize new upper bound to the current lower bound (already updated)
			ub = aggResult.getLowerBound(); 
			// Update upper bound with the latest scores from each queue where this result has not yet appeared
			for (String task : tasks.keySet()) {
				if ((!queues.get(task).isEmpty()) && (aggResult.getAppearance().get(this.similarities.get(task).getTaskId()) == false)) {
					ub += this.weights.get(task)[w] * queues.get(task).peek().getScore();
				}
			}
			
			// Insert new upper bound into the priority queue
			mapUpperBounds[w].put(ub, item); 	
			curResults[w].get(item).setUpperBound(ub);
		}
	}

	
	/**
	 * Implements the logic of the NRA (No Random Access) algorithm.
	 */
	@Override
	public IResult[][] proc(long query_timeout) {
		
		long startTime = System.currentTimeMillis();
		
		Double lb, ub = null;
		int n = 0;
	
		numTasks = tasks.size();
		BitSet probed = new BitSet(numTasks);

		// FETCH PHASE
		fetch();			
		
		// RANKING PHASE
		// Counter of ranked aggregate results issued so far
		int[] k = new int[weightCombinations];    // Each k is monitoring progress for each combination of weights
		Arrays.fill(k, 0);			
		
		// Iterate over candidates in the the priority queues
		// Stop any further examination if top-k results are issued or if the process times out	
		boolean stop = false;
		while (!stop && (System.currentTimeMillis() - startTime < query_timeout)) {
			
			probed.clear(); // New results to be fetched from each queue
			
			// Update ranked list with the next result from each queue
			for (String task : tasks.keySet()) {
				probed.set(this.similarities.get(task).getTaskId(), updateRankedList(task));
			}
			// If no updates have been made to the ranked list, then all queues have been exhausted and the process should be terminated
			if (probed.length() == 0) {
//				System.out.println("All queues have been exhausted!");
				break;
			}
			
			n++;	
//				if (n % 100 == 0)
//					System.out.print("Iteration #" + n + "..." + "\r");

			stop = true;
			// Examine current results for each combination of weights
			for (int w = 0; w < weightCombinations; w++) {		
				// Adjust the UPPER bounds in aggregated results
				updateUpperBounds(w);
				
				// Check whether the next result can be returned
				if (curResults[w].size() >= 1) {
					// Iterators are used to point to the head of priority queues
					// Identify the greatest lower bound and ...
					Iterator<Double> iterLowerBound = mapLowerBounds[w].keys().iterator(); 
					lb = iterLowerBound.next();
					// .. the greatest upper bound among the remaining items
					Iterator<Double> iterUpperBound = mapUpperBounds[w].keys().iterator();
					ub = iterUpperBound.next(); 
	
					// FIXME: Implements the progressive issuing of results; check if this condition is always safe
					// Issue next result once the greatest lower bound exceeds the upper bounds of all other candidates
					if (lb >= ub) {
						// Get the object identifier listed at the head of this priority queue
						String item = mapLowerBounds[w].values().iterator().next();
						ub = curResults[w].get(item).getUpperBound();
						
						// One more result can be issued for this combination of weights
						issueRankedResult(k[w], w, item, lb, true);   // Exact ranking
						k[w] = results[w].size();
//						log.writeln("RESULT: " + item + " " + lb + " " + ub);
						
						// Remove this result from the rank aggregation list and the priority queues
						mapLowerBounds[w].remove(lb, item);
						mapUpperBounds[w].remove(ub, item);
						curResults[w].remove(item);
					}
				}
				
				// Stop if results have been acquired for each combination of weights
				// OR if no further EXACT-scored results may be determined from the ranked list
				stop = stop && (( n > topk * Constants.INFLATION_FACTOR) ||  (k[w] >= topk));
			}
			
			// Once top-k ranked results are returned, stop all running tasks gracefully
			for (String task : tasks.keySet()) 
				runControl.get(task).set(false);   //Stop each task
		}
		
		// Report any extra results by descending lower bound
		reportExtraResultsLB();
						
		
		this.log.writeln("In total " + n + " results have been examined from each queue.");
		
		// Array of final results		
		IResult[][] allResults = new IResult[weightCombinations][topk];
		for (int w = 0; w < weightCombinations; w++) {
			allResults[w] = results[w].toArray();
//			this.log.writeln("LOWER BOUND queue -> insertions: " + mapLowerBounds[w].getNumInserts() + " deletions: " + mapLowerBounds[w].getNumDeletes() + " current size: " + mapLowerBounds[w].size());
//			this.log.writeln("UPPER BOUND queue -> insertions: " + mapUpperBounds[w].getNumInserts() + " deletions: " + mapUpperBounds[w].getNumDeletes() + " current size: " + mapUpperBounds[w].size());
		}
		
		return allResults;
	}

	
	/** 
	 * Complement top-k final results approximately by picking extra items with descending LOWER bounds on their aggregate scores.
	 */
	private void reportExtraResultsLB() {
		
		Double lb;
		// Examine current candidates for each combination of weights
		for (int w = 0; w < weightCombinations; w++) {
			boolean keepReporting = true;
			Iterator<Double> iterLowerBound = mapLowerBounds[w].keySet().iterator();
			// Probe by descending lower bounds
			while (iterLowerBound.hasNext() && keepReporting) {
				lb = iterLowerBound.next(); 
				for (String item: mapLowerBounds[w].get(lb)) {
					keepReporting = issueRankedResult(results[w].size(), w, item, lb, false);   // Such rankings should NOT be considered as exact
					if (!keepReporting)
						break;
				}
			}
		}	
	}
	
}
