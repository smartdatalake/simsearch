package eu.smartdatalake.simsearch.ranking;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

import eu.smartdatalake.simsearch.Result;

/**
 * Progressively computes ranked aggregated results according to the No Random Access algorithm.
 */
public class NoRandomAccessRanking implements RankAggregator {

	PrintStream logStream = null;
	
	AggregateResult aggResult;
	String val;
	double score;
	int numTasks;
	int topk;       // Number of ranked aggregated results to collect
 
	String COLUMN_DELIMITER = ";";    //Default delimiter for values in the output file
	PrintStream outStream;
	
	// Priority queues of results based on DESCENDING scores
	ListMultimap<Double, String> mapLowerBounds; 
	ListMultimap<Double, String> mapUpperBounds; 

	// List of ranked aggregated results
	HashMap<String, AggregateResult> rankedList;

	// List of queues that collect results from each running task
	List<ConcurrentLinkedQueue<Result>> queues;
	List<Thread> tasks;
	
	// List of atomic booleans to control execution of the various threads
	List<AtomicBoolean> runControl = new ArrayList<AtomicBoolean>();


	/**
	 * Constructor
	 * @param tasks   The list of running threads; each one executes a query and it is associated with its respective queue that collects its results.
	 * @param queues  The list of the queues collecting results from each search query.
	 * @param runControl  The list of boolean values indicating the status of each thread.
	 * @param topk  The count of ranked aggregated results to collect, i.e., those with the top-k (highest) aggregated similarity scores.
	 * @param outStream  Handle to the output file.
	 * @param outColumnDelimiter  Delimiter character of results written to the output file.
	 * @param outHeader  Header (column names) in the output file.
	 * @param logStream  Handle to the log file for notifications and execution statistics.
	 */
	public NoRandomAccessRanking(List<Thread> tasks, List<ConcurrentLinkedQueue<Result>> queues, List<AtomicBoolean> runControl, int topk, PrintStream outStream, String outColumnDelimiter, boolean outHeader, PrintStream logStream) {
		
		this.logStream = logStream;
		this.tasks = tasks;
		this.queues = queues;
		this.runControl = runControl;
		this.topk = topk;  
		
		this.outStream = outStream;
		
		//Write header to the output file
		if (outHeader)
			this.outStream.print("top" + COLUMN_DELIMITER + "id" + COLUMN_DELIMITER + "LowerBound" + COLUMN_DELIMITER + "UpperBound" + "\n");
			
		//Instantiate priority queues, one on DESCENDING lower bounds...
		mapLowerBounds = Multimaps.newListMultimap(new TreeMap<>(Collections.reverseOrder()), ArrayList::new); 
		//...and another on DESCENDING upper bounds of aggregated results
		mapUpperBounds = Multimaps.newListMultimap(new TreeMap<>(Collections.reverseOrder()), ArrayList::new); 
		
		// Instantiate list to hold ranked aggregated results
		rankedList = new HashMap<String, AggregateResult>();
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
			if (res != null) {
				// A common element must be used per result
				val = res.getId();
				score = res.getScore();
				aggResult = rankedList.get(val);
				if (aggResult != null) { // UPDATE: Aggregate result already
											// exists in the ranked list
					mapLowerBounds.remove(aggResult.getLowerBound(), val);
					// Update its lower bound
					aggResult.setLowerBound(aggResult.getLowerBound() + score); 
					// Mark that this result has also been retrieved from this queue
					aggResult.setAppearance(i); 
					// If all results have been received, the upper bound
					// coincides with the lower bound
					if (aggResult.checkAppearance()) {
						mapUpperBounds.remove(aggResult.getUpperBound(), val);
						aggResult.setUpperBound(aggResult.getLowerBound());
					}

					rankedList.put(val, aggResult);
					// Maintain updated bounds in the priority queues
					mapLowerBounds.put(aggResult.getLowerBound(), val);
					mapUpperBounds.put(aggResult.getUpperBound(), val);
				} else { // INSERT new aggregate result to the ranked list...
					aggResult = new AggregateResult(val, numTasks, score, score);
					// Mark that this result has also been retrieved from this queue
					aggResult.setAppearance(i); 
					rankedList.put(val, aggResult);
					// ...and put it to the priority queues holding lower and upper bounds
					mapLowerBounds.put(score, val); 
					mapUpperBounds.put(score, val);
				}
				return true;
			}
		}

		return false;
	}

	/**
	 * Updates upper bounds of ranked aggregated results at current iteration
	 */
	private void updateUpperBounds() {
		double ub;
		// Once a new result has been obtained from all queries, adjust the
		// UPPER bounds in each aggregated result
		for (String val : rankedList.keySet()) {
			aggResult = rankedList.get(val);
			// if (aggResult.checkAppearance()) //Upper bound is already fixed
			// for this aggregated result, since all partial results have been
			// received from all tasks
			// return;
			
			// Remove previous upper bound from the priority queue
			mapUpperBounds.remove(aggResult.getUpperBound(), val); 
			// Initialize upper bound to the current lower bound (already updated)
			ub = aggResult.getLowerBound(); 
			// Update upper bound with the latest scores from each queue where
			// this result has not yet appeared
			for (int i = 0; i < numTasks; i++) {
				if (aggResult.getAppearance().get(i) == false) {
					ub += queues.get(i).peek().getScore();
				}
			}
			// Insert new upper bound into the priority queue
			mapUpperBounds.put(ub, val); 
			rankedList.get(val).setUpperBound(ub);
		}
	}

	
	/**
	 * Implements the logic of the NRA (No Random Access) algorithm.
	 */
	@Override
	public boolean proc() {
		
		boolean running = true;
		int n = 0;
		
		try {
			numTasks = tasks.size();
			Double lb, ub;
			BitSet probed = new BitSet(numTasks);
			int k = 0;    // Number of ranked aggregated results so far
			
			while (running) {
				n++;
				running = false;
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
				
				if (n % 100 == 0)
					System.out.print("Iteration #" + n + "..." + "\r");
	
				// Since all queues have been updated with their next result,
				// adjust the UPPER bounds in aggregated results
				updateUpperBounds();
	
				// Once at least topk candidate aggregated results have been collected,
				// check whether the next result can be returned
				if (rankedList.size() > 1) {
					// Iterators are used to point to the head of priority queues
					// Identify the greatest lower bound and ...
					Iterator<Double> iterLowerBound = mapLowerBounds.keys().iterator(); 
					lb = iterLowerBound.next();
					// .. the greatest upper bound among the remaining items
					Iterator<Double> iterUpperBound = mapUpperBounds.keys().iterator();
//					iterUpperBound.next();    // FIXME: Is it safe to use the second largest upper bound?
					ub = iterUpperBound.next(); 
	
					// FIXME: Implements the progressive issuing of results;
					// check if this condition is always safe
					// If the greatest lower bound is higher than the upper
					// bounds of all other candidate results, then issue the
					// next final result
					if (lb >= ub) {
						k++;
						// Get the value listed at the head of this priority queue
						String val = mapLowerBounds.values().iterator().next();
						this.outStream.print(k + COLUMN_DELIMITER);
						ub = rankedList.get(val).getUpperBound();
						this.outStream.print(val + COLUMN_DELIMITER + lb + COLUMN_DELIMITER + ub + "\n");
//						System.out.println(val + " LB-> " + lb + " UB-> " + ub);
						// Remove this result from the ranked aggregation list
						// and the priority queues
						mapLowerBounds.remove(lb, val);
						mapUpperBounds.remove(ub, val);
						rankedList.remove(val);
					}
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
				
				//Once top-k ranked results are computed, stop all running tasks gracefully
				if (k == topk) {
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
