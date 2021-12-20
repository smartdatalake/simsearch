package eu.smartdatalake.simsearch.engine.processor.ranking;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
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
 * Implementation of a variant rank aggregation algorithm using random access to partially available look-ups obtained by each similarity search query.
 */
public class PartialRandomAccessRanking<K,V> extends RankAggregator<K,V> {

	String item;
	double[] threshold;
	long randomAccesses;

	// Priority queues of results based on DESCENDING scores
	AggregateScoreQueue[] mapLowerBounds; 
	AggregateScoreQueue[] mapUpperBounds;
	AggregateScoreQueue[] mapAverageBounds;
	
	// Retains the lowest scores obtained per priority queue
	Map<String, Double> lowestScores;
	
	// List with the identifiers of the checked objects (the same one for all combinations of weights)
	CheckedItems checkedItems;

	/**
	 * Constructor
	 * @param datasetIdentifiers  Dictionary of the attributes involved in similarity search queries.
	 * @param lookups  Look-up tables for attributes involved in the similarity search. This is NOT the entire collection of original attribute values, but built LOCALLY from the items in each priority queue.  
	 * @param similarities   Dictionary of the similarity measures applied in each search query.
	 * @param weights  Dictionary of the (possibly multiple alternative) weights per attribute to be applied in scoring the final results. 
	 * @param normalizations  Dictionary of normalization functions to be applied in data values during random access.
	 * @param tasks   The collection of running threads; each one executes a query and it is associated with its respective queue that collects its results.
	 * @param queues  The collection of ranked lists (priority queues) collecting results from each search query.
	 * @param runControl  The collection of boolean values indicating the status of each thread.
	 * @param topk  The count of ranked aggregated results to collect, i.e., those with the top-k (highest) aggregated similarity scores.
	 * @param log  Handle to the log file for notifications and execution statistics.
	 */
	public PartialRandomAccessRanking(Map<String, DatasetIdentifier> datasetIdentifiers, Map<String, Map<K, V>> lookups, Map<String, ISimilarity> similarities, Map<String, Double[]> weights, Map<String, INormal> normalizations, Map<String, Thread> tasks, Map<String, RankedList> queues, Map<String, AtomicBoolean> runControl, int topk, Logger log) {
	
		super(datasetIdentifiers, lookups, similarities, weights, normalizations, tasks, queues, runControl, topk, log);
	
		// Counter of random access requests to the lookups
		randomAccesses = 0;
		
		// Array of thresholds to consider at each iteration
		threshold = new double[weightCombinations];
				
		// Instantiate priority queues, a pair of queues per combination of weights
		// The first queue is on DESCENDING lower bounds...
		mapLowerBounds = new AggregateScoreQueue[weightCombinations]; 
		//...and the second on DESCENDING upper bounds of aggregated results
		mapUpperBounds = new AggregateScoreQueue[weightCombinations];
		//... and an extra one on the average of these bounds
		mapAverageBounds = new AggregateScoreQueue[weightCombinations];
	
		// Instantiate a list with the checked objects in order to avoid duplicate checks 
		// Duplicates could have been emitted if the same item had been returned again by another priority queue
		checkedItems = new CheckedItems();
		
		// Keeps the lowest scores observed per priority queue
		lowestScores = new HashMap<String, Double>();
		
		// Initialize all array structures
		for (int w = 0; w < weightCombinations; w++) {
			mapLowerBounds[w] = new AggregateScoreQueue(topk+1);
			mapUpperBounds[w] = new AggregateScoreQueue(topk+1);
			mapAverageBounds[w] = new AggregateScoreQueue(topk+1);
			curResults[w] = new AggregateResultCollection();
		}
		
	}
	

	/**
	 * Provides the next partial result from a queue. Results already examined due to random access are skipped.
	 * @param taskKey  The hashKey of the task to be checked for its next result.
	 * @return  A partial result from this queue to be used in rank aggregation.
	 */
	private PartialResult fetchPartialResult(String taskKey) {
		
		if (!queues.get(taskKey).isEmpty()) {
			PartialResult res = queues.get(taskKey).poll(); 	// Result removed from queue!
			while (res != null) {
				if (checkedItems.contains(res.getId().toString())) { // Result has already been examined before, ...  
					res = queues.get(taskKey).poll(); 				 // ... so get next result from that queue
				}
				else
					return res;
			}
		}
		return null;
	}
	
	
	/**
	 * Inserts or updates the ranked aggregated results based on a result from a queue.
	 * @param taskKey   The hashKey of the task (associated with a priority queue) to be checked for its next result.
	 * @return A Boolean value: True, if the ranked aggregated list has been updated; otherwise, False.
	 */
	private boolean updateRankedList(String taskKey) {

		PartialResult res = fetchPartialResult(taskKey); 	// Result removed from queue!
		if (res != null) {
			// A common identifier must be used per result
			item = res.getId().toString();
			//CAUTION! Weighted score used in aggregation
			for (int w = 0; w < weightCombinations; w++) {
				score = this.weights.get(taskKey)[w] * res.getScore();
				threshold[w] += score;
				aggResult = curResults[w].get(item);
				if (aggResult == null) {    // This is the first appearance of this item in any queue

					// INSERT new aggregate result to the ranked list...
					aggResult = new AggregateResult(item, numTasks, score, score);
					// Mark that this result has also been retrieved from this queue
					aggResult.setAppearance(this.similarities.get(taskKey).getTaskId()); 
					
					// Use random access to the other constructed look-ups in order to update the bounds
					updateBounds(aggResult, w);
//					log.writeln(taskKey + " " + aggResult.getId() + " --> " + aggResult.getLowerBound() + " --> " + aggResult.getUpperBound());
					
					// Put this candidate into the ranked list (its scores will not be updated afterwards) 
					curResults[w].put(item, aggResult);
					// ...and put it to the priority queues holding lower and upper bounds
					mapLowerBounds[w].put(aggResult.getLowerBound(), item); 
					mapUpperBounds[w].put(aggResult.getUpperBound(), item);
					mapAverageBounds[w].put(0.5*(aggResult.getLowerBound() + aggResult.getUpperBound()), item);
					// Remember that a result with this key has been examined
					checkedItems.add(item);
				}
			}
			return true;
		}

		return false;   // No further updates
	}

	
	/**
	 * Updates bounds regarding the score of a query result by randomly accessing any attribute values available in the partial lookups.
	 * @param res  The ranked candidate result.
	 * @param w   The identifier of the weight combination to be applied on the scores.
	 */
	private void updateBounds(AggregateResult res, int w) {
		
		double lb = res.getLowerBound();
		double ub = 0.0;

		for (String task : tasks.keySet()) {	
			//CAUTION! Weighted scores used in aggregation
			if (res.getAppearance().get(this.similarities.get(task).getTaskId()) == false) {
				Object val = this.lookups.get(task).get(res.getId());   // Access attribute value
				randomAccesses++;
				if (val != null) {   // If not null, then update score accordingly
					if (this.normalizations.get(task) != null)   // Apply normalization, if specified
						lb += this.weights.get(task)[w] * this.similarities.get(task).calc(this.normalizations.get(task).normalize(val));
					else
						lb += this.weights.get(task)[w] * this.similarities.get(task).calc(val);
					res.setAppearance(this.similarities.get(task).getTaskId());
				}
				else {   // No value found in the partial look-up, so use the lowest available score from this facet
					ub += this.weights.get(task)[w] * this.lowestScores.get(task);
				}
			}
		}
		
		// Update bounds
		res.setLowerBound(lb);
		res.setUpperBound(lb + ub);
	}
	
	
	/**
	 * Implements the logic of the Partial Random Access algorithm.
	 */
	@Override
	public IResult[][] proc(long query_timeout) {
		
		long startTime = System.currentTimeMillis();
		
		Double mb, lb, ub = null;
		int n = 0;
		PartialResult<K, V> pRes = null;
	
		numTasks = tasks.size();
		BitSet probed = new BitSet(numTasks);

		// FETCH PHASE
		fetch();
	
		// Populate look-ups with the contents of the priority queues
		// TODO: Integrate look-up in the fetch process
		for (String task : tasks.keySet()) {
	        Iterator<PartialResult> qIter = queues.get(task).iterator(); 
	        while (qIter.hasNext()) { 
	        	pRes = qIter.next();
	        	this.lookups.get(task).put(pRes.getId(), pRes.getValue());
	        }
	        lowestScores.put(task, pRes.getScore());    // Remember the score in the last element available in this queue
//		    log.writeln("Lookup for task " + task + " was populated with " + this.lookups.get(task).size() + " values.");	        
		}
		
		// RANKING PHASE
		// Counter of ranked aggregate results issued so far
		int[] k = new int[weightCombinations];    // Each k is monitoring progress for each combination of weights
		Arrays.fill(k, 0);
		
		// Iterate over candidates in the the priority queues
		// Stop any further examination if top-k results are issued or if the process times out	
		boolean stop = false;
		while (!stop && (System.currentTimeMillis() - startTime < query_timeout)) {
					
			// To be updated with results from this iteration
			for (int w = 0; w < weightCombinations; w++) {
				threshold[w] = 0.0;     // Reset thresholds for all combinations
			}
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
//			if (n % 100 == 0)
//				System.out.print("Iteration #" + n + "..." + "\r");
				
			stop = true;
			// Examine current results for each combination of weights
			for (int w = 0; w < weightCombinations; w++) {
				// Check whether the next result can be returned				
				if (curResults[w].size() > 1) {
					// Iterators are used to point to the head of priority queues
					// Identify the greatest lower bound and ...
					Iterator<Double> iterLowerBound = mapLowerBounds[w].keys().iterator(); 
					lb = iterLowerBound.next();
					//... the greatest upper bound among the remaining items
					Iterator<Double> iterUpperBound = mapUpperBounds[w].keys().iterator();
					ub = iterUpperBound.next(); 
					//... and also the greatest among the averages of bounds 
					Iterator<Double> iterAverageBound = mapAverageBounds[w].keys().iterator();
					mb = iterAverageBound.next(); 
					
					// FIXME: Implements the progressive issuing of results; check if this condition is always safe
					// Issue next result once the greatest lower bound exceeds the upper bounds of all other candidates
					// Also compare current threshold with the highest lower bound
					if ((lb >= mb) && (lb >= threshold[w])) {
						// Get the object identifier listed at the head of this priority queue
						String item = mapLowerBounds[w].values().iterator().next();
						ub = curResults[w].get(item).getUpperBound();
						
						// One more result can be issued for this combination of weights
						issueRankedResult(k[w], w, item, lb, true);   // Exact ranking
						k[w] = results[w].size();
//						log.writeln("RESULT: " + item + " " + lb + " " + ub + " " + threshold[w]);

						// Remove this result from the rank aggregation list and the priority queues
						mapLowerBounds[w].remove(lb, item);
						mapUpperBounds[w].remove(ub, item);
						mapAverageBounds[w].remove(0.5*(lb+ub), item);
						curResults[w].remove(item);
					}
				}
				// Stop if results have been acquired for each combination of weights
				// OR if no further EXACT-scored results may be determined from the ranked list
				stop = stop && ((n > topk * Constants.INFLATION_FACTOR) || (k[w] >= topk));
			}			
		}

		// Once top-k ranked results are returned, stop all running tasks gracefully
		for (String task : tasks.keySet()) 
			runControl.get(task).set(false);   //Stop each task

		// Report extra results by descending average bound
		reportExtraResultsMB();
					
/*		
		// NOT USED: Alternative methods to issue extra results:
		// Report extra results by descending upper bound
		reportExtraResultsUB();
	
		// Report extra results by descending lower bound
		reportExtraResultsLB();
*/
		this.log.writeln("In total " + n + " results have been examined from each queue.");
		this.log.writeln("Random accesses to lookup values: " + randomAccesses + " (for all weight combinations).");
		this.log.writeln("Last upper bound examined: " + ub);
		
		// Array of final results		
		IResult[][] allResults = new IResult[weightCombinations][topk];
		for (int w = 0; w < weightCombinations; w++) {
			allResults[w] = results[w].toArray();
//			this.log.writeln("LOWER BOUND queue -> insertions: " + mapLowerBounds[w].getNumInserts() + " deletions: " + mapLowerBounds[w].getNumDeletes() + " current size: " + mapLowerBounds[w].size());
//			this.log.writeln("UPPER BOUND queue -> insertions: " + mapUpperBounds[w].getNumInserts() + " deletions: " + mapUpperBounds[w].getNumDeletes() + " current size: " + mapUpperBounds[w].size());
//			this.log.writeln("AVERAGE BOUND queue -> insertions: " + mapAverageBounds[w].getNumInserts() + " deletions: " + mapAverageBounds[w].getNumDeletes() + " current size: " + mapAverageBounds[w].size());
		}	
		
		return allResults;
	}

	
	/** 
	 * Complement top-k final results approximately by picking extra items with descending UPPER bounds on their aggregate scores.
	 */
	private void reportExtraResultsUB() {
		
		Double ub;
		boolean keepReporting = true;

		// Examine current candidates for each combination of weights
		for (int w = 0; w < weightCombinations; w++) {
			int i = results[w].size();
			Iterator<Double> iterUpperBound = mapUpperBounds[w].keySet().iterator();
			// Probe by descending upper bounds
			while (iterUpperBound.hasNext() && keepReporting) {
				ub = iterUpperBound.next(); 
				for (String item: mapUpperBounds[w].get(ub)) {
					keepReporting =issueRankedResult(i, w, item, ub, false);  // These rankings should NOT be considered as exact
					if (!keepReporting)
						break;
				}
			}
		}	
	}
	

	/** 
	 * Complement top-k final results approximately by picking extra items with descending LOWER bounds on their aggregate scores.
	 */
	private void reportExtraResultsLB() {
		
		Double lb;
		boolean keepReporting = true;
		
		// Examine current candidates for each combination of weights
		for (int w = 0; w < weightCombinations; w++) {
			int i = results[w].size();
			Iterator<Double> iterLowerBound = mapLowerBounds[w].keySet().iterator();
			// Probe by descending lower bounds
			while (iterLowerBound.hasNext() && keepReporting) {
				lb = iterLowerBound.next(); 
				for (String item: mapLowerBounds[w].get(lb)) {
					keepReporting = issueRankedResult(i, w, item, lb, false);  // These rankings should NOT be considered as exact	
					if (!keepReporting)
						break;
				}
			}
		}	
	}


	/** 
	 * Complement top-k final results approximately by picking extra items with descending AVERAGE bounds on their aggregate scores.
	 */
	private void reportExtraResultsMB() {
		
		Double mb;	
		
		// Examine current candidates for each combination of weights
		for (int w = 0; w < weightCombinations; w++) {
			boolean keepReporting = true;
			Iterator<Double> iterAvgBound = mapAverageBounds[w].keySet().iterator();
			// Probe by descending upper bounds
			while (iterAvgBound.hasNext() && keepReporting) {
				mb = iterAvgBound.next(); 
				for (String item: mapAverageBounds[w].get(mb)) {
					keepReporting = issueRankedResult(results[w].size(), w, item, mb, false);  // Such rankings should NOT be considered as exact
					if (!keepReporting)
						break;
				}
			}
		}	
	}

}
