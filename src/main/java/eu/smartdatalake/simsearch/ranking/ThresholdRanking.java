package eu.smartdatalake.simsearch.ranking;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

import eu.smartdatalake.simsearch.ISimilarity;
import eu.smartdatalake.simsearch.Result;
import eu.smartdatalake.simsearch.numerical.INormal;

/**
 * Implementation of the threshold-based rank aggregation algorithm.
 */
public class ThresholdRanking implements RankAggregator {

	//FIXME: Check which types of similarity measures should apply
	public final static int SIMILARITY_JACCARD = 0;   // used for categorical top-k
	public final static int SIMILARITY_SPATIAL = 1;   //TODO: spatial similarity not yet implemented
	public final static int SIMILARITY_NUMERICAL = 2;  // used for numerical top-k
	
	PrintStream logStream = null;
	
	AggregateResult aggResult;
	String val;
	double score;
	int numTasks;
	int topk;       // Number of ranked aggregated results to collect
	double threshold;
 
	String COLUMN_DELIMITER = ";";    //Default delimiter for values in the output file
	PrintStream outStream;
	
	// List of ranked aggregated results at current iteration
	HashMap<String, AggregateResult> curResults;

	// Priority queue of ranked results based on DESCENDING scores
	ListMultimap<Double, String> rankedList; 
	
	// List of queues that collect results from each running task
	List<ConcurrentLinkedQueue<Result>> queues;
	List<Thread> tasks;
	
	// Weights
	List<Double> weights;
	
	// Normalization functions
	List<INormal> normalizations;
	
	//Dictionaries built for the various datasets as needed in random access similarity calculations
	List<HashMap<?,?>> datasets;
	
	// List of atomic booleans to control execution of the various threads
	List<AtomicBoolean> runControl = new ArrayList<AtomicBoolean>();

	// List of similarity functions to be used in random access calculations
	List<ISimilarity> similarities;
	
	/**
	 * Constructor
	 * @param datasets   List of the various data collections involved in the similarity search queries.
	 * @param similarities   List of the similarity measures applied in each search query.
	 * @param weights  List of the weights to be applied in similarity scores returned by each search query.
	 * @param normalizations  List of normalization functions to be applied in data values during random access.
	 * @param tasks  The list of running threads; each one executes a query and it is associated with its respective queue that collects its results.
	 * @param queues  The list of the queues collecting results from each search query.
	 * @param runControl  The list of boolean values indicating the status of each thread.
	 * @param topk  The count of ranked aggregated results to collect, i.e., those with the top-k (highest) aggregated similarity scores.
	 * @param outStream  Handle to the output file.
	 * @param outColumnDelimiter   Delimiter character of results written to the output file.
	 * @param outHeader  Header (column names) in the output file.
	 * @param logStream  Handle to the log file for notifications and execution statistics.
	 */
	public ThresholdRanking(List<HashMap<?,?>> datasets, List<ISimilarity> similarities, List<Double> weights, List<INormal> normalizations, List<Thread> tasks, List<ConcurrentLinkedQueue<Result>> queues, List<AtomicBoolean> runControl, int topk, PrintStream outStream, String outColumnDelimiter, boolean outHeader, PrintStream logStream) {
		
		this.logStream = logStream;
		this.datasets = datasets;
		this.similarities = similarities;
		this.tasks = tasks;
		this.queues = queues;
		this.weights = weights;
		this.normalizations = normalizations;
		this.runControl = runControl;
		this.topk = topk;
			
		this.outStream = outStream;
		
		//Write header to the output file
		if (outHeader) {
			this.outStream.print("top" + COLUMN_DELIMITER + "id" + COLUMN_DELIMITER);
			//FIXME: Is it possible to replace placeholders with original attribute names?
			for (int j = 0; j < tasks.size(); j++) {   
				this.outStream.print("attrVal" + (j+1) + COLUMN_DELIMITER);
				this.outStream.print("attrScore" + (j+1) + COLUMN_DELIMITER);
			}
			this.outStream.print("totalScore" + "\n");
		}
			
		// Instantiate priority queue to hold ranked results on DESCENDING scores
		rankedList = Multimaps.newListMultimap(new TreeMap<>(Collections.reverseOrder()), ArrayList::new);
	}


	/**
	 * Inserts or updates the ranked aggregated results based on a result from
	 * the i-queue
	 * 
	 * @param i
	 *            The queue that provides the new result
	 * @return True, if the ranked aggregated list has been updated; otherwise,
	 *         False.
	 */
	private boolean updateRankedList(int i) {

		if (!queues.get(i).isEmpty()) {
			Result res = queues.get(i).peek(); // Do NOT remove this result from
												// queue
//			System.out.println("QUEUE: " + i);
			if (res != null) {
				// A common element must be used per result
				val = res.getId();
				//CAUTION! Weighted score used in aggregation
				score = this.weights.get(i) * res.getScore();   
				threshold += score;
				aggResult = curResults.get(val);
				// INSERT new aggregate result to the ranked list if not already exists
				if (aggResult != null) { // UPDATE: Aggregate result already
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
					curResults.put(val, aggResult);			
				} else { // INSERT new aggregate result to the ranked list...
					aggResult = new AggregateResult(val, numTasks, score, score);
					// Mark that this result has also been retrieved from this queue
					aggResult.setAppearance(i); 
					curResults.put(val, aggResult);
				}
				return true;
			}
		}

		return false;
	}
	
	// 
	/**
	 * Calculates the total aggregated score of a query result by randomly accessing its attribute values.
	 * @param res  The query result.
	 * @return  The aggregated score after estimating the missing scores in unseen attributes.
	 */
	private double updateScoreRandomAccess(AggregateResult res) {
		
		double score = res.getLowerBound();
		BitSet appearances = res.getAppearance();
		for (int i = 0; i < numTasks; i++) {
			//CAUTION! Weighted scores used in aggregation
			if (appearances.get(i) == false) {
				Object val = this.datasets.get(i).get(res.getId());   // Access attribute value
				if (val != null) {   // If not null, then update score accordingly
					if (this.normalizations.get(i) != null)   // Apply normalization, if specified
						score += this.weights.get(i) * this.similarities.get(i).calc(this.normalizations.get(i).normalize(val));
					else
						score += this.weights.get(i) * this.similarities.get(i).calc(val);
				}
			}
		}
		return score;
	}
	

	/**
	 * Implements the processing logic of the threshold-based algorithm.
	 */
	@Override
	public boolean proc() {
	
		boolean running = true;
		int n = 0;
		
		try {
			numTasks = tasks.size();			
			BitSet probed = new BitSet(numTasks);
			int k = 0;    // Number of ranked aggregated results so far
			
			while (running) {
				n++;
				running = false;
				threshold = 0.0;  // To be updated with results from this iteration
				curResults = new HashMap<String, AggregateResult>();
				probed.clear(); // New result to be fetched from each queue
	
				// Wait until all queues have been updated with their n-th
				// result
				while (probed.cardinality() < numTasks) {
					// Wait a while and then try again to fetch results
					// from the remaining queues
					TimeUnit.NANOSECONDS.sleep(1); 
					for (int i = 0; i < numTasks; i++) {
						if (probed.get(i) == false)
							probed.set(i, updateRankedList(i));
					}
				}
				
				// Since all queues have been updated with their next result,
				// do RANDOM ACCESS and compute the scores of the seen n-th results
				for (AggregateResult res: curResults.values()) {
					res.setLowerBound(updateScoreRandomAccess(res));
					//Refresh ranked aggregate results from current iteration
					//FIXME: Must check if this key already exists in the ranked results?
					rankedList.put(res.getLowerBound(), res.getId());
				}
					
//				if (n % 100 == 0)
//					System.out.print("Iteration #" + n + "... " + "\r"); //+ " #KEYS: " + rankedList.keySet().size() + " #ITEMS:" + rankedList.size() 
				
				// Remove excessive elements from the ranked list, once it contains more than top-k items
				if (rankedList.size() > topk) {
					int c = 0;
					double curScore;
					ArrayList<Double> listKeys = new ArrayList<Double>(rankedList.keySet());
					// Iterate from the highest score currently in the ranked list
					ListIterator<Double> iterKey = listKeys.listIterator(0);   
					while (iterKey.hasNext()) {
						curScore = iterKey.next();
						if (c < topk)
							c += rankedList.get(curScore).size();
						else   //Remove excessive elements
							rankedList.removeAll(curScore);
					}
				}
					
				// Once at least top-k candidate aggregated results have been collected,
				// check whether the next result can be issued
				Iterator<Double> iter = rankedList.keys().iterator(); 
//				System.out.println(rankedList.size() + " results collected. Max score: " + iter.next() + " Current threshold: " + threshold);
			
				// Report results
				if (rankedList.size() > k) {

					//Compare current threshold with the highest score at the head of the priority queue
					score = iter.next();  
					if ( score >= threshold) {
						// Report the items listed with this score
						List<String> values = rankedList.get(score);
						for (String val: values) {
							k++;
							// Report the rank and the identifier of the resulting item...
							this.outStream.print(k + COLUMN_DELIMITER + val + COLUMN_DELIMITER);
							// ... also its original values at the searched attributes
							for (int j = 0; j < numTasks; j++) {
								// attribute value
								this.outStream.print(this.datasets.get(j).get(val).toString() + COLUMN_DELIMITER);    
								// its individual similarity score
								if (this.normalizations.get(j) != null)   // Apply normalization, if specified
									this.outStream.print(this.similarities.get(j).calc(this.normalizations.get(j).normalize(this.datasets.get(j).get(val))) + COLUMN_DELIMITER);
								else
									this.outStream.print(this.similarities.get(j).calc(this.datasets.get(j).get(val)) + COLUMN_DELIMITER);  
							}
							//... and its aggregated score
							this.outStream.print(score + "\n");
						}
						// Remove this result from the ranked aggregation list
						rankedList.removeAll(score);
					}
/*					
					else {
						System.out.println(rankedList.size() + " results collected... " + threshold);
//						break;
					}	
*/
				}
	
				// Determine whether to continue polling results from queues
				for (int i = 0; i < numTasks; i++) {
					// Remove already processed result from this queue
					queues.get(i).poll(); 
//					running = running || (tasks.get(i).isAlive()) || !queues.get(i).isEmpty();
					running = running || !queues.get(i).isEmpty();
					// FIXME: Once a queue is exhausted, no more aggregated
					// results can be produced
					// TODO: Should the process be revised to continue
					// searching with the remaining queues?
					if (queues.get(i).isEmpty()) {
						this.logStream.println("Task " + i + " terminated!");
						running = false; // Quit the ranked aggregation process
						break;
					}
				}
				
				//Once top-k ranked results are returned, stop all running tasks gracefully
				if (k >= topk) {
					for (int i = 0; i < numTasks; i++) {
						runControl.get(i).set(false);   //Stop each task
					}
					running = false; 		// Quit the ranked aggregation process
				}
	
			}
		} catch (Exception e) { // InterruptedException
			e.printStackTrace();
		}
		finally {
			this.outStream.close();
		}
		
		this.logStream.println("In total " + n + " results have been examined from each queue.");
		
		return running;
	}

}
