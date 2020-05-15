package eu.smartdatalake.simsearch.ranking;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.DatasetIdentifier;
import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.PartialResult;
import eu.smartdatalake.simsearch.csv.numerical.INormal;
import eu.smartdatalake.simsearch.measure.ISimilarity;

/**
 * Progressively computes ranked aggregated results according to the No Random Access algorithm.
 */
public class NoRandomAccessRanking implements IRankAggregator {

	Logger log = null;
	
	AggregateResult aggResult;
	String item;
	double score;
	int numTasks;
	int topk;       // Number of ranked aggregated results to collect
 
	// Weights
	Map<String, Double[]> weights;
	int weightCombinations;
	
	// Priority queues of results based on DESCENDING scores
	AggregateScoreQueue[] mapLowerBounds; 
	AggregateScoreQueue[] mapUpperBounds;

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

	// Look-up tables built for the various datasets as needed in random access similarity calculations
	// Using the dataset hash key as a reference to the collected values for each attribute
	Map<String, HashMap<?, ?>> datasets;
		
	// Collection of similarity functions to be used in random access calculations
	Map<String, ISimilarity> similarities;
	
	// Collection of the ranked results to be given as output per weight combination
	RankedResultCollection[] results;
	
	/**
	 * Constructor
	 * @param datasetIdentifiers List of the attributes involved in similarity search queries.
	 * @param datasets   List of the various data collections involved in the similarity search queries.
	 * @param similarities   List of the similarity measures applied in each search query.
	 * @param weights  List of the weights to be applied in similarity scores returned by each search query.
	 * @param normalizations  List of normalization functions to be applied in data values during random access.
	 * @param tasks   The list of running threads; each one executes a query and it is associated with its respective queue that collects its results.
	 * @param queues  The list of the queues collecting results from each search query.
	 * @param runControl  The list of boolean values indicating the status of each thread.
	 * @param topk  The count of ranked aggregated results to collect, i.e., those with the top-k (highest) aggregated similarity scores.
	 * @param log  Handle to the log file for notifications and execution statistics.
	 */
	public NoRandomAccessRanking(Map<String, DatasetIdentifier> datasetIdentifiers, Map<String, HashMap<?, ?>> datasets, Map<String, ISimilarity> similarities, Map<String, Double[]> weights, Map<String, INormal> normalizations, Map<String, Thread> tasks, Map<String, ConcurrentLinkedQueue<PartialResult>> queues, Map<String, AtomicBoolean> runControl, int topk, Logger log) {
		
		this.log = log;
		this.datasetIdentifiers = datasetIdentifiers;
		this.datasets = datasets;
		this.weights = weights;
		this.similarities = similarities;
		this.normalizations = normalizations;
		this.tasks = tasks;
		this.queues = queues;
		this.runControl = runControl;
		this.topk = topk;  
		
		// Number of combinations of weights to apply
		weightCombinations = weights.entrySet().iterator().next().getValue().length;
		
		// Instantiate priority queues, a pair of queues per combination of weights
		// The first queue is on DESCENDING lower bounds...
		mapLowerBounds = new AggregateScoreQueue[weightCombinations]; 
		//...and the second on DESCENDING upper bounds of aggregated results
		mapUpperBounds = new AggregateScoreQueue[weightCombinations];

		// Array of aggregated results collected after the current iteration for each combination of weights
		curResults = new AggregateResultCollection[weightCombinations];
		
		// Array of collection of final ranked results; one collection (list) per combination of weights
		results = new RankedResultCollection[weightCombinations];
		
		// Initialize all array structures
		for (int w = 0; w < weightCombinations; w++) {
			mapLowerBounds[w] = new AggregateScoreQueue(topk+1);
			mapUpperBounds[w] = new AggregateScoreQueue(topk+1);
			curResults[w] = new AggregateResultCollection();
			results[w] = new RankedResultCollection();
		}
	}
	

	/**
	 * Inserts or updates the ranked aggregated results based on a result from the i-the queue.
	 * @param taskKey   The hashKey of the task to be checked for its next result.
	 * @param i   The queue that provides the new result
	 * @param w   The identifier of the weight combination to be applied on the scores.
	 * @return True, if the ranked aggregated list has been updated; otherwise, False.
	 */
	private boolean updateRankedList(String taskKey, int i, int w) {

		if (!queues.get(taskKey).isEmpty()) {
			PartialResult res = queues.get(taskKey).peek(); // Do NOT remove this result from queue
			if (res != null) {
				// A common identifier must be used per result
				item = res.getId().toString();
				//CAUTION! Weighted score used in aggregation
				score = this.weights.get(taskKey)[w] * res.getScore();
				aggResult = curResults[w].get(item);
				if (aggResult != null) { 	// UPDATE: Aggregate result already
											// exists in the ranked list
					mapLowerBounds[w].remove(aggResult.getLowerBound(), item);
					// Update its lower bound
					aggResult.setLowerBound(aggResult.getLowerBound() + score); 
					// Mark that this result has also been retrieved from this queue
					aggResult.setAppearance(i); 
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
					aggResult.setAppearance(i); 
					curResults[w].put(item, aggResult);
					// ...and put it to the priority queues holding lower and upper bounds
					mapLowerBounds[w].put(score, item); 
					mapUpperBounds[w].put(score, item);
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
			// Initialize upper bound to the current lower bound (already updated)
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
	public RankedResult[][] proc() {
		
		boolean running = true;
		int n = 0;
		
		try {
			numTasks = tasks.size();
			Double lb, ub;
			BitSet probed = new BitSet(numTasks);
			// Number of ranked aggregated results so far
			int[] k = new int[weightCombinations];    // Each k is monitoring progress for each combination of weights
			Arrays.fill(k, 0);
			
			int i;
			boolean allScalesSet = false;
			long startTime = System.currentTimeMillis();
			
			while (running) {
				
				// Wait until all scale factors for similarity scores have been set in all partial results from each task
				// Otherwise, scale factors may have been set arbitrarily due to the random access calculations per attribute.
				while (allScalesSet == false) {
					allScalesSet = true;
					TimeUnit.NANOSECONDS.sleep(100);
					for (String task : tasks.keySet()) {
						// In case that the task is completed and no scale has been set, this means that all results are exact and no scale is needed
						allScalesSet = allScalesSet && (!runControl.get(task).get() || this.similarities.get(task).isScaleSet());
					}
				}
				
				n++;
				running = false;
				probed.clear(); // New result to be fetched from each queue
				i = -1;
	
				// Wait until all queues have been updated with their n-th result
				while (probed.cardinality() < numTasks) {
					// Wait a while and then try again to fetch results from the remaining queues
//					TimeUnit.NANOSECONDS.sleep(1); 				
					for (String task : tasks.keySet()) {
						i = this.similarities.get(task).getTaskId();
						if (probed.get(i) == false)
							for (int w = 0; w < weightCombinations; w++)
								probed.set(i, updateRankedList(task, i, w));
					}
				}
				
//				if (n % 100 == 0)
//					System.out.print("Iteration #" + n + "..." + "\r");
	
				// Since all examined queues have been updated with their next result,
				// ...adjust the UPPER bounds in aggregated results
				for (int w = 0; w < weightCombinations; w++) {
					updateUpperBounds(w);
				}
	
				// Examine current results for each combination of weights
				boolean stop = true;
				for (int w = 0; w < weightCombinations; w++) {
					// Once at least topk candidate aggregated results have been collected,
					// check whether the next result can be returned
					
					if (curResults[w].size() > 1) {
						// Iterators are used to point to the head of priority queues
						// Identify the greatest lower bound and ...
						Iterator<Double> iterLowerBound = mapLowerBounds[w].keys().iterator(); 
						lb = iterLowerBound.next();
						// .. the greatest upper bound among the remaining items
						Iterator<Double> iterUpperBound = mapUpperBounds[w].keys().iterator();
	//					iterUpperBound.next();    // FIXME: Would it be safe to use the second largest upper bound?
						ub = iterUpperBound.next(); 
		
						// FIXME: Implements the progressive issuing of results; check if this condition is always safe
						// Issue next result once the greatest lower bound exceeds the upper bounds of all other candidates
						if (lb >= ub) {
							k[w]++;
							// Get the object identifier listed at the head of this priority queue
							String item = mapLowerBounds[w].values().iterator().next();
							ub = curResults[w].get(item).getUpperBound();
							
							// One more result can be issued for this combination of weights
							issueRankedResult(k[w], w, item, lb);
//							log.writeln("RESULT: " + item + " " + lb + " " + ub);
							
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
				
				// Determine whether to continue polling results from queues
				// Once top-k ranked results are returned, stop all running tasks gracefully
				// If the process times out, then stop any further examination
				if (stop || (System.currentTimeMillis() - startTime > Constants.RANKING_MAX_TIME)) {
					for (String task : tasks.keySet()) {
						runControl.get(task).set(false);   //Stop each task
					}
					running = false; 		// Quit the ranked aggregation process
				} else {  // Otherwise, if at least one task is still running, then continue searching
					numTasks = 0;					
					for (String task : tasks.keySet()) {
						queues.get(task).poll();    // Remove already processed result from this queue
						// Once a queue is exhausted, it is no longer considered and search continues with the remaining queues
						if (tasks.get(task).isAlive() || !queues.get(task).isEmpty()) {						
							running = true;	
							numTasks++;
						}
					}
				}
			}
/*			
			// Report extra results by descending lower bound
			reportExtraResultsLB();
*/			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.print(n + ",");  // Count iterations for experimental results
		this.log.writeln("In total " + n + " results have been examined from each queue.");
		
		// Array of final results		
		RankedResult[][] allResults = new RankedResult[weightCombinations][topk];
		for (int w = 0; w < weightCombinations; w++) {
			allResults[w] = results[w].toArray();
		}	
		
		return allResults;
	}

	/** 
	 * Complement top-k final results by picking extra items with descending LOWER bounds on their aggregate scores.
	 */
	private void reportExtraResultsLB() {
		
		Double lb;
		log.writeln("-----------Extra-results-by-LOWER-bound-------------------");
		// Examine current candidates for each combination of weights
		for (int w = 0; w < weightCombinations; w++) {
			int i = results[w].size();
			Iterator<Double> iterLowerBound = mapLowerBounds[w].keySet().iterator();
			// Probe by descending lower bounds
			while (iterLowerBound.hasNext()) {
				lb = iterLowerBound.next(); 
				for (String item: mapLowerBounds[w].get(lb)) {
					if (i < topk) {
						log.writeln(item + " LB: " + curResults[w].get(item).getLowerBound() + " UB: "+ curResults[w].get(item).getUpperBound());  //+ " THR: " + threshold[w]
						i++;
						issueRankedResult(i, w, item, lb);
					}
					else
						return;
				}
			}
		}	
	}
	
	/**
	 * Inserts the i-th ranked result to the output list. Rank is based on the overall score; ties in scores are resolved arbitrarily.
	 * @param w  The identifier of the weight combination to be applied on the scores.
	 * @param item   The original identifier of this item in the dataset
	 * @param rank	 The rank of this result in the output list.
	 * @param score  The overall (weighted) score of this result.
	 */
	private void issueRankedResult(int i, int w, String item, double score) {
		
		// Create a new resulting item and report its rank and its original identifier
		// Include values and scores for individual attribute; this is also needed for the similarity matrix
		RankedResult res = new RankedResult(tasks.size());
		res.setId(item);
		res.setRank(i);
		
		// ... also its original values at the searched attributes and the calculated similarity scores
		int j = 0;
		for (String task : tasks.keySet()) {
			Attribute attr = new Attribute();
			attr.setName(this.datasetIdentifiers.get(task).getColumnName());
			if (this.datasets.get(task).get(item) == null) {   	 // By default, assign zero similarity for NULL values in this attribute							
				attr.setValue("");   // Use blank string instead of NULL
				attr.setScore(0.0);
			}
			else {	  // Not NULL attribute value   
				attr.setValue(this.datasets.get(task).get(item).toString());
				// Estimate its individual similarity score
				if (this.normalizations.get(task) != null)   // Apply normalization, if specified
					attr.setScore(this.similarities.get(task).calc(this.normalizations.get(task).normalize(this.datasets.get(task).get(item))));
				else
					attr.setScore(this.similarities.get(task).calc(this.datasets.get(task).get(item)));
			}
			res.attributes[j] = attr;
			j++;
		}
		// Its aggregated score is the lower bound
		res.setScore(score);
		
		// Issue result to the output queue
		results[w].add(res);
	}	
	
}
