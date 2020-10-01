package eu.smartdatalake.simsearch.ranking;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import eu.smartdatalake.simsearch.IValueFinder;
import eu.smartdatalake.simsearch.Assistant;
import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.DatasetIdentifier;
import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.PartialResult;
import eu.smartdatalake.simsearch.csv.numerical.INormal;
import eu.smartdatalake.simsearch.measure.ISimilarity;

/**
 * Implementation of the threshold-based rank aggregation algorithm.
 */
public class ThresholdRanking implements IRankAggregator {
	
	Logger log = null;
	Assistant myAssistant;
	
	AggregateResult aggResult;
	String val;
	double score;
	int numTasks;
	int topk;       // Number of ranked aggregated results to collect
	double[] threshold;
	long valueProbes;
	long randomAccesses;
 
	// Array of collections with ranked aggregated results at current iteration; one collection per weight combination
	AggregateResultCollection[] curResults;

	// Array of priority queues of ranked results based on DESCENDING scores; one queue per weight combination
	AggregateScoreQueue[] scoreQueues; 
	
	// Collection of queues that collect results from each running task
	Map<String, ConcurrentLinkedQueue<PartialResult>> queues;
	Map<String, Thread> tasks;
	
	// Collection of all data/index datasetIdentifiers involved in the search
	Map<String, DatasetIdentifier> datasetIdentifiers;
	
	// Weights
	Map<String, Double[]> weights;
	int weightCombinations;
	
	// Normalization functions
	Map<String, INormal> normalizations;
	
	// Look-up tables built for the various datasets as needed in random access similarity calculations
	// Using the dataset hash key as a reference to the collected values for each attribute
	Map<String, HashMap<?, ?>> datasets;
	
	// Collection of value finder instantiations for random access to the full data sources (DBMSs / REST APIs)
	Map<String, IValueFinder> valueFinders;
	
	// Collection of atomic booleans to control execution of the various threads
	Map<String, AtomicBoolean> runControl = new HashMap<String, AtomicBoolean>();

	// Collection of similarity functions to be used in random access calculations
	Map<String, ISimilarity> similarities;
	
	// List with the identifiers of the checked objects (the same one for all combinations of weights)
	CheckedItems checkedItems;
	
	// Collection of the ranked results to be given as output per weight combination
	RankedResultCollection[] results;
	
	
	/**
	 * Constructor
	 * @param datasetIdentifiers List of the attributes involved in similarity search queries.
	 * @param datasets   Dictionary of the various data collections involved in the similarity search queries.
	 * @param similarities   Dictionary of the similarity measures applied in each search query.
	 * @param weights  Dictionary of the weights to be applied in similarity scores returned by each search query.
	 * @param normalizations  Dictionary of normalization functions to be applied in data values during random access.
	 * @param tasks  Dictionary of running threads; each one executes a query and it is associated with its respective queue that collects its results.
	 * @param queues  Dictionary of the queues collecting results from each search query.
	 * @param valueFinders  Dictionary of the random access operations available for the attributes involved in the similarity search.
	 * @param runControl  Dictionary of boolean values indicating the status of each thread.
	 * @param topk  The count of ranked aggregated results to collect, i.e., those with the top-k (highest) aggregated similarity scores.
	 * @param log  Handle to the log file for notifications and execution statistics.
	 */
	public ThresholdRanking(Map<String, DatasetIdentifier> datasetIdentifiers, Map<String, HashMap<?, ?>> datasets, Map<String, ISimilarity> similarities, Map<String, Double[]> weights, Map<String, INormal> normalizations, Map<String, Thread> tasks, Map<String, ConcurrentLinkedQueue<PartialResult>> queues, Map<String, IValueFinder> valueFinders, Map<String, AtomicBoolean> runControl, int topk, Logger log) {
		
		myAssistant = new Assistant();
		this.log = log;
		this.datasetIdentifiers = datasetIdentifiers;
		this.datasets = datasets;
		this.valueFinders = valueFinders;
		this.similarities = similarities;
		this.tasks = tasks;
		this.queues = queues;
		this.weights = weights;
		this.normalizations = normalizations;
		this.runControl = runControl;
		this.topk = topk;
		
		// Counter of queries (to a DBMS or REST API) to retrieve values required for random access operations
		valueProbes = 0;
		// Counter of random access requests to the lookups
		randomAccesses = 0;
		
		// Number of combinations of weights to apply
		weightCombinations = weights.entrySet().iterator().next().getValue().length;
		
		// Instantiate array of priority queues to hold ranked results on DESCENDING scores; one queue per combination of weights
		scoreQueues = new AggregateScoreQueue[weightCombinations];
		
		// Array of thresholds to consider at each iteration
		threshold = new double[weightCombinations];
		
		// Array of aggregated results collected at the current iteration for each combination of weights
		curResults = new AggregateResultCollection[weightCombinations];
		
		// Instantiate a list with the checked objects in order to avoid duplicate checks 
		// Duplicates could have been emitted if the same item had been returned again by another priority queue
		checkedItems = new CheckedItems();

		// Array of collection of results; one collection (list) per combination of weights
		results = new RankedResultCollection[weightCombinations];
		
		// Initialize all array structures
		for (int w = 0; w < weightCombinations; w++) {
			scoreQueues[w] = new AggregateScoreQueue(topk+1);
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
			PartialResult res = queues.get(taskKey).peek(); // Result NOT removed from queue!
			while (res != null) {
				if (checkedItems.contains(res.getId().toString())) { 	// Result has already been examined before, so...  
					queues.get(taskKey).poll();    				// ...remove it from this queue
					res = queues.get(taskKey).peek(); 			// ... and get next result from that queue
				}
				else
					return res;
			}
		}
		return null;
	}

	/**
	 * Inserts or updates the ranked aggregated results based on a result from the i-th queue
	 * @param taskKey   The hashKey of the task to be checked for its next result.
	 * @param i   The queue that provides the new result.
	 * @param w   The identifier of the weight combination to be applied on the scores.
	 * @return  A Boolean value: True, if the ranked aggregated list has been updated; otherwise, False.
	 */
	private boolean updateRankedList(String taskKey, int i, int w) {

		PartialResult res = fetchPartialResult(taskKey); // Result NOT removed from queue!
		if (res != null) {
			// A common identifier must be used per result
			val = res.getId().toString();
			//CAUTION! Weighted score used in aggregation
			score = this.weights.get(taskKey)[w] * res.getScore();   
			threshold[w] += score;
			aggResult = curResults[w].get(val);
			// Handle new aggregate result to the ranked list
			if (aggResult != null) { 	// UPDATE: Aggregate result already
										// exists in the ranked list
				// Update its lower bound
				aggResult.setLowerBound(aggResult.getLowerBound() + score); 
				// Mark that this result has also been retrieved from this queue
				aggResult.setAppearance(i); 
				// If all results have been received, the upper bound
				// coincides with the lower bound
				if (aggResult.checkAppearance()) {
					aggResult.setUpperBound(aggResult.getLowerBound());
				}			
				curResults[w].put(val, aggResult);
			} else { 	// INSERT new aggregate result to the ranked list...
				aggResult = new AggregateResult(val, numTasks, score, score);
				// Mark that this result has also been retrieved from this queue
				aggResult.setAppearance(i); 
				curResults[w].put(val, aggResult);
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
				Object val = this.datasets.get(task).get(res.getId());   // Access attribute value
				randomAccesses++;
				if (val == null) {
					
					//if (this.datasetIdentifiers.get(task).getDataSource().getJdbcConnPool() != null) 
					if (this.datasetIdentifiers.get(task).getDataSource().isInSitu()) {
						// Retrieval from the DBMS or REST API also updates the in-memory data look-up
						val = valueFinders.get(this.datasetIdentifiers.get(task).getHashKey()).find(this.datasets.get(task), res.getId());
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
	public RankedResult[][] proc() {
			
		boolean running = true;
		int n = 0;
		
		try {
			numTasks = tasks.size();	
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
			
				// To be updated with results from this iteration
				for (int w = 0; w < weightCombinations; w++) {
					threshold[w] = 0.0;     // Reset thresholds for all combinations
					curResults[w] = new AggregateResultCollection();
				}
				probed.clear(); // New results to be fetched from each queue
	
				// Wait until all examined queues have been updated with their n-th result
				i = -1;
				while (probed.cardinality() < numTasks) {
					// Wait a while and then try again to fetch results from the remaining queues
					TimeUnit.NANOSECONDS.sleep(1); 
					for (String task : tasks.keySet()) {
						i = this.similarities.get(task).getTaskId();
						if (probed.get(i) == false)
							for (int w = 0; w < weightCombinations; w++) 
								probed.set(i, updateRankedList(task, i, w));
					}
					if (System.currentTimeMillis() - startTime > Constants.RANKING_MAX_TIME)
						break;
				}
				
				// Since all examined queues have been updated with their next result,...
				// ...do RANDOM ACCESS and compute the scores of the seen n-th results
				for (int w = 0; w < weightCombinations; w++) {
					for (AggregateResult res: curResults[w].values()) {
						res.setLowerBound(updateScoreRandomAccess(res, w));
						// Remember that a result with this key has been examined
						checkedItems.add(res.getId().toString());
						//Refresh scores of ranked aggregate results from current iteration
						scoreQueues[w].put(res.getLowerBound(), res.getId().toString());
					}
				}
					
//				if (n % 100 == 0)
//					System.out.print("Iteration #" + n + "... " + scoreQueues[0].size() + "\r"); //+ " #KEYS: " + rankedList.keySet().size() + " #ITEMS:" + rankedList.size() 
				
				// Examine current results for each combination of weights
				boolean stop = true;
				for (int w = 0; w < weightCombinations; w++) {
					
					// If candidate aggregated results have been collected,
					// check whether the next result can be issued
					if (scoreQueues[w].size() > 0) {
						Iterator<Double> iter = scoreQueues[w].keys().iterator(); 

						//Compare current threshold with the highest score at the head of the priority queue
						score = iter.next();  
						if (score >= threshold[w]) {
							// One more result can be issued for this combination of weights
							issueRankedResult(k[w], w, true);    // Exact ranking
							k[w] = results[w].size();
							// Remove this result from the respective priority queue of aggregate scores
							scoreQueues[w].removeAll(score);
						}
					}
			
					// Stop if results have been acquired for each combination of weights
					// OR if no further EXACT-scored results may be determined from the ranked list
					stop = stop && (( n > topk * Constants.INFLATION_FACTOR) || (k[w] >= topk));  
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
			
			// Report any extra results by descending scores
			reportExtraResults();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
//		System.out.print(n + ",");  // Count iterations for experimental results
		this.log.writeln("In total " + n + " results have been examined from each queue.");		
		this.log.writeln("Random accesses to lookup values: " + randomAccesses + " (for all weight combinations). Extra values retrieved from original data sources: " + valueProbes);
		
		// Prepare array of final results
		RankedResult[][] allResults = new RankedResult[weightCombinations][topk];
		for (int w = 0; w < weightCombinations; w++) {
			allResults[w] = results[w].toArray();
		}	
		
		return allResults;
	}

	
	/** 
	 * Complement top-k final results by picking extra items with descending aggregate scores from the queue.
	 */
	private void reportExtraResults() {
		
		boolean keepReporting = true;
//		log.writeln("-----------Extra-results-by-largest-score-------------------");
		// Examine current candidates for each combination of weights
		for (int w = 0; w < weightCombinations; w++) {
			if (results[w].size() >= topk)
				continue;
			Iterator<Double> iterScore = scoreQueues[w].keys().iterator(); 
			// Probe by descending aggregate scores
			while (iterScore.hasNext() && keepReporting) {
				score = iterScore.next(); 
				keepReporting = issueRankedResult(results[w].size(), w, false);   // Such rankings should NOT be considered as exact
			}
		}	
	}

	
	/**
	 * Inserts the i-th ranked result to the output list. Rank is based on the overall score; ties in scores are resolved arbitrarily.
	 * @param i  The rank to the assigned to the output result.
	 * @param w   The identifier of the weight combination to be applied on the scores.
	 * @param exact  Boolean indicating whether the ranking of this result is exact or not.
	 * @return  A Boolean value: True, if extra result(s) have been issued; otherwise, False.
	 */
	private boolean issueRankedResult(int i, int w, boolean exact) {
		// Report the items listed with this score
		List<String> items = scoreQueues[w].get(score);
		for (String it: items) {
			
			i++;  // Showing rank as 1,2,3,... instead of 0,1,2,...
			// Stop once topk results have been issued, even though there might be ties having the same score as the topk result
			if (i > topk)
				return false;
			
			// Create a new resulting item and report its rank and its original identifier...
			RankedResult res = new RankedResult(tasks.size());
			res.setId(it);
			res.setRank(i);
			
			// ... also its original values at the searched attributes and the calculated similarity scores
			int j = 0;
			for (String task : tasks.keySet()) {
				Attribute attr = new Attribute();
				attr.setName(this.datasetIdentifiers.get(task).getValueAttribute());
				if (this.datasets.get(task).get(it) == null) {   	 // By default, assign zero similarity for NULL values in this attribute							
					attr.setValue("");   // Use blank string instead of NULL
					attr.setScore(0.0);
				}
				else {	  // Not NULL attribute value   
					attr.setValue(myAssistant.formatAttrValue(this.datasets.get(task).get(it)));
					// Estimate its individual similarity score
					if (this.normalizations.get(task) != null)   // Apply normalization, if specified
						attr.setScore(this.similarities.get(task).calc(this.normalizations.get(task).normalize(this.datasets.get(task).get(it))));
					else
						attr.setScore(this.similarities.get(task).calc(this.datasets.get(task).get(it)));
				}
				res.attributes[j] = attr;
				j++;
			}
			//... and its overall aggregated score
			res.setScore(score / tasks.size());   // Aggregate score over all running tasks (one per queried attribute)
		
			// Indicate whether this ranking should be considered exact or not
			res.setExact(exact);
			
			// Issue result to the output queue
			results[w].add(res);
//			log.writeln(i + " RESULT: " + res.getId() + " " + res.getScore());
		}
		return true;
	}
	
}
