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
import eu.smartdatalake.simsearch.engine.processor.IValueFinder;
import eu.smartdatalake.simsearch.manager.DatasetIdentifier;
import eu.smartdatalake.simsearch.manager.ingested.numerical.INormal;

/**
 * Implementation of the threshold-based rank aggregation algorithm.
 */
public class ThresholdRanking<K, V> extends RankAggregator<K, V> {

	String val;
	double[] threshold;
	long valueProbes;
	long randomAccesses;  
	
	// Array of priority queues of ranked results based on DESCENDING scores; one queue per weight combination
	AggregateScoreQueue[] scoreQueues; 
	
	// Collection of value finder instantiations for random access to the full data sources (DBMSs / REST APIs)
	Map<String, IValueFinder> valueFinders;

	// List with the identifiers of the checked objects (the same one for all combinations of weights)
	CheckedItems checkedItems;

	/**
	 * Constructor
	 * @param datasetIdentifiers List of the attributes involved in similarity search queries.
	 * @param lookups   Dictionary of the various data collections involved in the similarity search queries.
	 * @param similarities   Dictionary of the similarity measures applied in each search query.
	 * @param weights  Dictionary of the (possibly multiple alternative) weights per attribute to be applied in scoring the final results. 
	 * @param normalizations  Dictionary of normalization functions to be applied in data values during random access.
	 * @param tasks  Collection of running threads; each one executes a query and it is associated with its respective queue that collects its results.
	 * @param queues  Collection of ranked lists (priority queues) collecting results from each search query.
	 * @param valueFinders  Dictionary of the random access operations available for the attributes involved in the similarity search.
	 * @param runControl  Collection of boolean values indicating the status of each thread.
	 * @param topk  The count of ranked aggregated results to collect, i.e., those with the top-k (highest) aggregated similarity scores.
	 * @param log  Handle to the log file for notifications and execution statistics.
	 */
	public ThresholdRanking(Map<String, DatasetIdentifier> datasetIdentifiers, Map<String, Map<K, V>> lookups, Map<String, ISimilarity> similarities, Map<String, Double[]> weights, Map<String, INormal> normalizations, Map<String, Thread> tasks, Map<String, RankedList> queues, Map<String, IValueFinder> valueFinders, Map<String, AtomicBoolean> runControl, int topk, Logger log) {
		
		super(datasetIdentifiers, lookups, similarities, weights, normalizations, tasks, queues, runControl, topk, log);

		this.valueFinders = valueFinders;
		
		// Instantiate array of priority queues to hold ranked results on DESCENDING scores; one queue per combination of weights
		scoreQueues = new AggregateScoreQueue[weightCombinations];
		
		// Counter of queries (to a DBMS or REST API) to retrieve values required for random access operations
		valueProbes = 0;
		// Counter of random access requests to the lookups
		randomAccesses = 0;
		
		// Array of thresholds to consider at each iteration
		threshold = new double[weightCombinations];
	
		// Instantiate a list with the checked objects in order to avoid duplicate checks 
		// Duplicates could have been emitted if the same item had been returned again by another priority queue
		checkedItems = new CheckedItems();

		// Initialize array structures
		for (int w = 0; w < weightCombinations; w++) {
			scoreQueues[w] = new AggregateScoreQueue(topk+1);
		}
		
	}

	
	/**
	 * Provides the next partial result from a queue. Results already examined due to random access are skipped.
	 * @param taskKey  The hashKey of the task to be checked for its next result.
	 * @return  A partial result from this queue to be used in rank aggregation.
	 */
	private PartialResult fetchPartialResult(String taskKey) {
		
		if (!(queues.get(taskKey)).isEmpty()) {
			PartialResult res = queues.get(taskKey).poll(); // Result NOT removed from queue!
			while (res != null) {
				if (checkedItems.contains(res.getId().toString())) { 	// Result has already been examined before, so...  
					res = queues.get(taskKey).poll(); 					// ... get next result from that queue
				}
				else
					return res;
			}
		}
		return null;
	}

	
	/**
	 * Inserts or updates the ranked aggregated results based on a result from a queue.
	 * @param taskKey   The hashKey of the task to be checked for its next result.
	 * @return  A Boolean value: True, if the ranked aggregated list has been updated; otherwise, False.
	 */
	private boolean updateRankedList(String taskKey) {

		PartialResult res = fetchPartialResult(taskKey); // Result NOT removed from queue!
		if (res != null) {
			// A common identifier must be used per result
			val = res.getId().toString();
			//CAUTION! Weighted score used in aggregation
			for (int w = 0; w < weightCombinations; w++) {
				score = this.weights.get(taskKey)[w] * res.getScore();   
				threshold[w] += score;
				aggResult = curResults[w].get(val);
				// Handle new aggregate result to the ranked list
				if (aggResult != null) { 	// UPDATE: Aggregate result already
											// exists in the ranked list
					// Update its lower bound
					aggResult.setLowerBound(aggResult.getLowerBound() + score); 
					// Mark that this result has also been retrieved from this queue
					aggResult.setAppearance(this.similarities.get(taskKey).getTaskId()); 
					// If all results have been received, the upper bound
					// coincides with the lower bound
					if (aggResult.checkAppearance()) {
						aggResult.setUpperBound(aggResult.getLowerBound());
					}			
					curResults[w].put(val, aggResult);
				} else { 	// INSERT new aggregate result to the ranked list...
					aggResult = new AggregateResult(val, numTasks, score, score);
					// Mark that this result has also been retrieved from this queue
					aggResult.setAppearance(this.similarities.get(taskKey).getTaskId()); 
					curResults[w].put(val, aggResult);
				}
			}
			return true;
		}

		return false;
	}


	/**
	 * Calculates the total aggregated score of a query result by randomly accessing its attribute values.
	 * @param res  The query result.
	 * @param w   The identifier of the weight combination to be applied on the scores.
	 * @return  The aggregated score after estimating the missing scores in unseen attributes.
	 */
	private double updateScoreRandomAccess(AggregateResult res, int w) {
		
		double score = res.getLowerBound();
		BitSet appearances = res.getAppearance();
		for (String task : tasks.keySet()) {	
			// Need to perform random access for unprobed attributes
			if (appearances.get(this.similarities.get(task).getTaskId()) == false) {
				Object val = this.lookups.get(task).get(res.getId());   // Access attribute value
				randomAccesses++;
				if (val == null) {
					
					//if (this.datasetIdentifiers.get(task).getDataSource().getJdbcConnPool() != null) 
					if (this.datasetIdentifiers.get(task).getDataSource().isInSitu()) {
						// Retrieval from the DBMS or REST API also updates the in-memory data look-up
						val = valueFinders.get(this.datasetIdentifiers.get(task).getHashKey()).find(this.lookups.get(task), res.getId());
						valueProbes++;
					}
//					else if (this.datasetIdentifiers.get(task).getDataSource().getHttpConn() != null) {
//						System.out.println("Currently no support for random access to REST API");   // TODO
//					}
				}
				//CAUTION! Weighted scores used in aggregation
				if (val != null) {   // If value is not null, then update score accordingly
					if (this.normalizations.get(task) != null)   // Apply normalization, if specified
						score += this.weights.get(task)[w] * this.similarities.get(task).calc(this.normalizations.get(task).normalize(val));
					else
						score += this.weights.get(task)[w] * this.similarities.get(task).calc(val);
					// Mark this candidate as probed in this attribute
					res.setAppearance(this.similarities.get(task).getTaskId());
				}
			}
		}
		return score;
	}

	
	/**
	 * Implements the processing logic of the threshold-based algorithm.
	 */
	@Override
	public IResult[][] proc() {
			
		long startTime = System.currentTimeMillis();
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
		while (!stop && (System.currentTimeMillis() - startTime < Constants.RANKING_MAX_TIME)) {

			// To be updated with results from this iteration
			for (int w = 0; w < weightCombinations; w++) {
				threshold[w] = 0.0;     // Reset thresholds for all combinations
				curResults[w] = new AggregateResultCollection();
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
			
			// Apply RANDOM ACCESS and compute the scores of the seen n-th candidates
			for (int w = 0; w < weightCombinations; w++) {
				for (AggregateResult res: curResults[w].values()) {
					res.setLowerBound(updateScoreRandomAccess(res, w));
					// Remember that a result with this key has been examined
					checkedItems.add(res.getId().toString());
					//Refresh scores of ranked aggregate results from current iteration
					scoreQueues[w].put(res.getLowerBound(), res.getId().toString());
				}
			}
				
			n++;
//			if (n % 100 == 0)
//				System.out.print("Iteration #" + n + "... " + scoreQueues[0].size() + "\r"); //+ " #KEYS: " + rankedList.keySet().size() + " #ITEMS:" + rankedList.size() 
			
			stop = true;
			// Examine current results for each combination of weights
			for (int w = 0; w < weightCombinations; w++) {
				
				// If candidate aggregated results have been collected,
				// check whether the next result can be issued
				if (scoreQueues[w].size() > 0) {
					Iterator<Double> iter = scoreQueues[w].keys().iterator(); 

					//Compare current threshold with the highest score at the head of the priority queue
					score = iter.next();  
					if (score >= threshold[w]) {
						// Report the items listed with this score
						for (String item: scoreQueues[w].get(score)) {
							// One more result can be issued for this combination of weights
							issueRankedResult(k[w], w, item, score, true);    // Exact ranking
							k[w] = results[w].size();
						}
						// Remove this result from the respective priority queue of aggregate scores
						scoreQueues[w].removeAll(score);
					}
				}
		
				// Stop if results have been acquired for each combination of weights
				// OR if no further EXACT-scored results may be determined from the ranked list
				stop = stop && (( n > topk * Constants.INFLATION_FACTOR) || (k[w] >= topk));  
			}
		}
		
		// Once top-k ranked results are returned, stop all running tasks gracefully
		for (String task : tasks.keySet()) 
			runControl.get(task).set(false);   //Stop each task
		
		// Report any extra results by descending scores
		reportExtraResults();
		
		this.log.writeln("In total " + n + " results have been examined from each queue.");		
		this.log.writeln("Random accesses to lookup values: " + randomAccesses + " (for all weight combinations). Extra values retrieved from original data sources: " + valueProbes);
		
		// Prepare array of final results
		IResult[][] allResults = new IResult[weightCombinations][topk];
		for (int w = 0; w < weightCombinations; w++) {
			allResults[w] = results[w].toArray();
//			this.log.writeln("SCORES queue -> insertions: " + scoreQueues[w].getNumInserts() + " deletions: " + scoreQueues[w].getNumDeletes() + " current size: " + scoreQueues[w].size());
		}	
		
		return allResults;
	}

	/** 
	 * Complement top-k final results approximately by picking extra items with descending aggregate scores from the queue.
	 */
	private void reportExtraResults() {
	
		// Examine current candidates for each combination of weights
		for (int w = 0; w < weightCombinations; w++) {
			boolean keepReporting = true;
			if (results[w].size() >= topk)
				continue;		
			Iterator<Double> iterScore = scoreQueues[w].keys().iterator(); 
			// Probe by descending aggregate scores
			while (iterScore.hasNext() && keepReporting) {
				score = iterScore.next(); 
				for (String item: scoreQueues[w].get(score)) {
					keepReporting = issueRankedResult(results[w].size(), w, item, score, false);   // Such rankings should NOT be considered as exact
				}
			}
		}	
	}

	
}
