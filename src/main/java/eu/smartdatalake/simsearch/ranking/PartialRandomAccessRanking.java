package eu.smartdatalake.simsearch.ranking;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;

import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.DatasetIdentifier;
import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.PartialResult;
import eu.smartdatalake.simsearch.csv.numerical.INormal;
import eu.smartdatalake.simsearch.measure.ISimilarity;

/**
 * Implementation of a variant rank aggregation algorithm using random access to partially available look-ups obtained by each similarity search query..
 */
public class PartialRandomAccessRanking<K,V> implements IRankAggregator {

	Logger log = null;
	
	AggregateResult aggResult;
	String item;
	double score;
	int numTasks;
	int topk;       // Number of ranked aggregated results to collect
	double[] threshold;
	long randomAccesses;
 
	// Weights
	Map<String, Double[]> weights;
	int weightCombinations;
	
	// Priority queues of results based on DESCENDING scores
	AggregateScoreQueue[] mapLowerBounds; 
	AggregateScoreQueue[] mapUpperBounds;
	AggregateScoreQueue[] mapAverageBounds;

	// Array of collections with ranked aggregated results at current iteration; one collection per weight combination
	AggregateResultCollection[] curResults;
	
	// Collection of queues that collect results from each running task
	Map<String, ConcurrentLinkedQueue<PartialResult>> queues;
	Map<String, Thread> tasks;
	
	// Collection of atomic booleans to control execution of the various threads
	Map<String, AtomicBoolean> runControl = new HashMap<String, AtomicBoolean>();

	// Normalization functions
	Map<String, INormal> normalizations;
		
	// Collection of all attributes involved in the search
	Map<String, DatasetIdentifier> datasetIdentifiers;

	// Look-up tables built from the atrribute values available from each priority queue
	// IMPORTANT! This is NOT the whole collection of values for this attribute as in the original data
	// Using a hash key as a reference to the collected values per attribute (the same key as in other structures)
	Map<String, HashMap<K, V>> lookups;
		
	// Collection of similarity functions to be used in random access calculations
	Map<String, ISimilarity> similarities;
	
	// Collection of the ranked results to be given as output per weight combination
	RankedResultCollection[] results;
	
	// Retains the lowest scores obtained per priority queue
	Map<String, Double> lowestScores;
	
	// Retains the standard deviations of the scores obtained per priority queue
	Map<String, Double> stDev;
	
	// List with the identifiers of the checked objects (the same one for all combinations of weights)
	CheckedItems checkedItems;
	
	/**
	 * Constructor
	 * @param datasetIdentifiers List of the attributes involved in similarity search queries.
	 * @param lookups   Look-up tables for attributes involved in the similarity search.
	 * @param similarities   List of the similarity measures applied in each search query.
	 * @param weights  List of the weights to be applied in similarity scores returned by each search query.
	 * @param normalizations  List of normalization functions to be applied in data values during random access.
	 * @param tasks   The list of running threads; each one executes a query and it is associated with its respective queue that collects its results.
	 * @param queues  The list of the queues collecting results from each search query.
	 * @param runControl  The list of boolean values indicating the status of each thread.
	 * @param topk  The count of ranked aggregated results to collect, i.e., those with the top-k (highest) aggregated similarity scores.
	 * @param log  Handle to the log file for notifications and execution statistics.
	 */
	public PartialRandomAccessRanking(Map<String, DatasetIdentifier> datasetIdentifiers, Map<String, HashMap<K, V>> lookups, Map<String, ISimilarity> similarities, Map<String, Double[]> weights, Map<String, INormal> normalizations, Map<String, Thread> tasks, Map<String, ConcurrentLinkedQueue<PartialResult>> queues, Map<String, AtomicBoolean> runControl, int topk, Logger log) {
		
		this.log = log;
		this.datasetIdentifiers = datasetIdentifiers;
		this.lookups = lookups;
		this.weights = weights;
		this.similarities = similarities;
		this.normalizations = normalizations;
		this.tasks = tasks;
		this.queues = queues;
		this.runControl = runControl;
		this.topk = topk;  
		
		// Counter of random access requests to the lookups
		randomAccesses = 0;
	
		// Number of combinations of weights to apply
		weightCombinations = weights.entrySet().iterator().next().getValue().length;
			
		// In case now eights have been specified, they will be automatically determined from the partial results per queue
		if (weightCombinations < 1)
			weightCombinations = 1;
		
		// Array of thresholds to consider at each iteration
		threshold = new double[weightCombinations];
				
		// Instantiate priority queues, a pair of queues per combination of weights
		// The first queue is on DESCENDING lower bounds...
		mapLowerBounds = new AggregateScoreQueue[weightCombinations]; 
		//...and the second on DESCENDING upper bounds of aggregated results
		mapUpperBounds = new AggregateScoreQueue[weightCombinations];
		//... and an extra one on the average of these bounds
		mapAverageBounds = new AggregateScoreQueue[weightCombinations];

		// Array of aggregated results collected after the current iteration for each combination of weights
		curResults = new AggregateResultCollection[weightCombinations];
		
		// Instantiate a list with the checked objects in order to avoid duplicate checks 
		// Duplicates could have been emitted if the same item had been returned again by another priority queue
		checkedItems = new CheckedItems();
		
		// Array of collection of final ranked results; one collection (list) per combination of weights
		results = new RankedResultCollection[weightCombinations];
		
		// Keeps the lowest scores observed per priority queue
		lowestScores = new HashMap<String, Double>();
		
		// Keeps the standard deviations of the scores obtained per priority queue
		stDev = new HashMap<String, Double>();
		
		// Initialize all array structures
		for (int w = 0; w < weightCombinations; w++) {
			mapLowerBounds[w] = new AggregateScoreQueue(topk+1);
			mapUpperBounds[w] = new AggregateScoreQueue(topk+1);
			mapAverageBounds[w] = new AggregateScoreQueue(topk+1);
			curResults[w] = new AggregateResultCollection();
			results[w] = new RankedResultCollection();
		}
	}
	

	/**
	 * Provides the next partial result from a queue. Results already examined due to random access are skipped.
	 * @param taskKey  The hashKey of the task to be checked for its next result.
	 * @return  A partial result from this queue to be used in rank aggregation.
	 */
	private PartialResult fetchPartialResult(String taskKey) {
		
		if (!queues.get(taskKey).isEmpty()) {
			PartialResult res = queues.get(taskKey).poll(); // Result removed from queue!
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
	 * Inserts or updates the ranked aggregated results based on a result from the i-the queue.
	 * @param taskKey   The hashKey of the task (associated with a priority queue) to be checked for its next result.
	 * @return True, if the ranked aggregated list has been updated; otherwise, False.
	 */
	private boolean updateRankedList(String taskKey) {

		PartialResult res = fetchPartialResult(taskKey); // Result removed from queue!
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
	public RankedResult[][] proc() {
		
		Double mb, lb, ub = null;
		boolean running = true;
		int n = 0;
		boolean allScalesSet = false;
		PartialResult<K, V> pRes = null;
		
		try {
			numTasks = tasks.size();
			BitSet probed = new BitSet(numTasks);
			
			// LOOK-UP PHASE
			// Wait until all similarity search queries are concluded ...
			// ...and the results in their respective priority queues are finalized
			while (running) {
				running = false;
				// Wait until all scale factors for similarity scores have been set in all partial results from each task
				// Otherwise, scale factors may have been set arbitrarily.
				while (allScalesSet == false) {
					allScalesSet = true;
					TimeUnit.NANOSECONDS.sleep(100);
					for (String task : tasks.keySet()) {
						// In case that the task is completed and no scale has been set, this means that all results are exact and no scale is needed
						allScalesSet = allScalesSet && (!runControl.get(task).get() || this.similarities.get(task).isScaleSet());
					}
				}
				// Wait until all queries have finished
				for (String task : tasks.keySet()) {
					if (tasks.get(task).isAlive())
						running = true;
				}
			}
			
			// Populate look-ups with the contents of the priority queues
			StandardDeviation sd = new StandardDeviation();
			for (String task : tasks.keySet()) {
		        Iterator<PartialResult> qIter = queues.get(task).iterator(); 
		        List<Double> scores = new ArrayList<Double>();
		        while (qIter.hasNext()) { 
		        	pRes = qIter.next();
		        	this.lookups.get(task).put(pRes.getId(), pRes.getValue());
		        	scores.add(pRes.getScore());  
		        }
		        lowestScores.put(task, pRes.getScore());    // Remember the score in the last element available in this queue
//		        log.writeln("Lookup for task " + task + " was populated with " + this.lookups.get(task).size() + " values.");	        
		        // Also calculate the standard deviation per priority queue
				stDev.put(task, sd.evaluate(scores.stream().mapToDouble(d -> d).toArray()));
//				log.writeln("Standard deviation for task " + task + " -->" + stDev.get(task));
			}
			
			// If no weights have been specified, they will be automatically determined via the standard deviation of the scores per priority queue
			if (weights.entrySet().iterator().next().getValue().length == 0) {			
				// Sum up all standard deviations
				double totalStDev = stDev.values().stream().mapToDouble(a -> a).sum();
				// Assign a ratio over the total standard deviation as the weight on this attribute
				for (String task : tasks.keySet()) {
					weights.put(task, new Double[] {totalStDev/stDev.get(task)});
					log.writeln("Weight assigned on task " + task + " -->" + (totalStDev/stDev.get(task)));
				}				
			}			
			
			// RANKING PHASE
			// Counter of ranked aggregate results
			int[] k = new int[weightCombinations];    // Each k is monitoring progress for each combination of weights
			Arrays.fill(k, 0);
			
			long startTime = System.currentTimeMillis();
			boolean stop = false;

			// Iterate over candidates in the the priority queues
			// Stop any further examination if top-k results are issued or if the process times out	
			while (!stop && (System.currentTimeMillis() - startTime < Constants.RANKING_MAX_TIME)) {
						
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
//					System.out.println("All queues have been exhausted!");
					break;
				}
				
				n++;				
//				if (n % 100 == 0)
//					System.out.print("Iteration #" + n + "..." + "\r");
					
				stop = true;
				// Examine current results for each combination of weights
				for (int w = 0; w < weightCombinations; w++) {
					// Once at least topk candidate aggregated results have been collected,
					// check whether the next result can be returned				
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
//							log.writeln("RESULT: " + item + " " + lb + " " + ub + " " + threshold[w]);

							// Remove this result from the rank aggregation list and the priority queues
							mapLowerBounds[w].remove(lb, item);
							mapUpperBounds[w].remove(ub, item);
							mapAverageBounds[w].remove(0.5*(lb+ub), item);
							curResults[w].remove(item);
						}
					}
					// Stop if results have been acquired for each combination of weights
					// OR if no further EXACT-scored results may be determined from the ranked list
					stop = stop && ((n > topk * Constants.INFLATION_FACTOR) || (k[w] >= topk));     // || (curResults[w].size() == unprobed) 
				}			
			}

			// Once top-k ranked results are returned, stop all running tasks gracefully
			for (String task : tasks.keySet()) 
				runControl.get(task).set(false);   //Stop each task
/*				
			// Report extra results by descending upper bound
			reportExtraResultsUB();
		
			// Report extra results by descending lower bound
			reportExtraResultsLB();
*/
			// Report extra results by descending average bound
			reportExtraResultsMB();
					
		} catch (Exception e) {
			e.printStackTrace();
		}
		
//		System.out.print(n + ",");  // Count iterations for experimental results
		this.log.writeln("In total " + n + " results have been examined from each queue.");
		this.log.writeln("Random accesses to lookup values: " + randomAccesses + " (for all weight combinations).");
		this.log.writeln("Last upper bound examined: " + ub);
		
		// Array of final results		
		RankedResult[][] allResults = new RankedResult[weightCombinations][topk];
		for (int w = 0; w < weightCombinations; w++) {
			allResults[w] = results[w].toArray();
		}	
		
		return allResults;
	}

	/** 
	 * Complement top-k final results by picking extra items with descending UPPER bounds on their aggregate scores.
	 */
	private void reportExtraResultsUB() {
		
		Double ub;
		boolean keepReporting = true;
//		log.writeln("-----------Extra-results-by-UPPER-bound-------------------");
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
	 * Complement top-k final results by picking extra items with descending LOWER bounds on their aggregate scores.
	 */
	private void reportExtraResultsLB() {
		
		Double lb;
		boolean keepReporting = true;
//		log.writeln("-----------Extra-results-by-LOWER-bound-------------------");
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
	 * Complement top-k final results by picking extra items with descending AVERAGE bounds on their aggregate scores.
	 */
	private void reportExtraResultsMB() {
		
		Double mb;	
//		log.writeln("-----------Extra-results-by-AVERAGE-bound-------------------");
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

	
	/**
	 * Inserts the i-th ranked result to the output list. Rank is based on the overall score; ties in scores are resolved arbitrarily.
	 * @param i  The rank to the assigned to the output result.
	 * @param w  The identifier of the weight combination to be applied on the scores.
	 * @param item   The original identifier of this item in the dataset
	 * @param rank	 The rank of this result in the output list.
	 * @param score  The overall (weighted) score of this result.
	 * @param exact  Boolean indicating whether the ranking of this result is exact or not.
	 * @return  True, if extra result(s) have been issued; otherwise, False.
	 */
	private boolean issueRankedResult(int i, int w, String item, double score, boolean exact) {
		
		i++;   // Showing rank as 1,2,3,... instead of 0,1,2,...
		// Stop once topk results have been issued, even though there might be ties having the same score as the topk result
		if (i > topk)
			return false;
		
		// Create a new resulting item and report its rank and its original identifier
		// Includes values and scores per individual attribute; this is also needed for the similarity matrix
		RankedResult res = new RankedResult(tasks.size());
		res.setId(item);
		res.setRank(i);
		
		// ... also its original values at the searched attributes and the calculated similarity scores
		int j = 0;
		for (String task : tasks.keySet()) {
			Attribute attr = new Attribute();
			attr.setName(this.datasetIdentifiers.get(task).getValueAttribute());
			if (this.lookups.get(task).get(item) == null) {   	 // By default, assign zero similarity for NULL values in this attribute							
				attr.setValue("");   // Use blank string instead of NULL
				attr.setScore(0.0);
			}
			else {	  // Not NULL attribute value   
				attr.setValue(this.lookups.get(task).get(item).toString());
				// Estimate its individual similarity score
				if (this.normalizations.get(task) != null)   // Apply normalization, if specified
					attr.setScore(this.similarities.get(task).calc(this.normalizations.get(task).normalize(this.lookups.get(task).get(item))));
				else
					attr.setScore(this.similarities.get(task).calc(this.lookups.get(task).get(item)));
			}
			res.attributes[j] = attr;
			j++;
		}
		// Its aggregated score is the lower bound
		res.setScore(score / tasks.size());  // Aggregate score over all running tasks (one per queried attribute)
		
		// Indicate whether this ranking should be considered exact or not
		res.setExact(exact);
		
		// Issue result to the output queue
		results[w].add(res);
//		log.writeln(i + " RESULT: " + res.getId() + " " + res.getScore());
		
		return true;
	}
	
}
