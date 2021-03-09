package eu.smartdatalake.simsearch.ranking;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.TreeMap;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Multiset;

/**
 * Auxiliary class implementing a priority queue of the ranked results according to their aggregate scores in DESCENDING order.
 */
public class AggregateScoreQueue {
	
	private ListMultimap<Double, String> scoreQueue;
	private int capacity;
	private int numInserts;
	private int numDeletes;

	/**
	 * Constructor
	 * @param topk  The capacity (i.e., the number of key-value pairs) of this priority queue.
	 */
	public AggregateScoreQueue(int topk) {
		scoreQueue = Multimaps.newListMultimap(new TreeMap<>(Collections.reverseOrder()), ArrayList::new);
		capacity = topk;
		numInserts = 0;
		numDeletes = 0;
	}
	
	public ListMultimap<Double, String> getScoreQueue() {
		return scoreQueue;
	}

	public void setScoreQueue(ListMultimap<Double, String> scoreQueue) {
		this.scoreQueue = scoreQueue;
	}
	
	public boolean put(Double key, String value) {
		
		scoreQueue.put(key, value);
		numInserts++;

		// Remove excessive elements from the priority queue of scores, once its capacity is exceeded 
		int c = scoreQueue.keySet().size();

		ArrayList<Double> listKeys = new ArrayList<Double>(scoreQueue.keySet());
		// Iterate from the lowest score currently held in the ranked list
		ListIterator<Double> iterKey = listKeys.listIterator(c);   
		while ((c > this.capacity) && iterKey.hasPrevious()) {
			// Remove excessive elements having the lowest score
			numDeletes += scoreQueue.removeAll(iterKey.previous()).size();    
			c--;
		}			
		
		return true;
	}
	
	public int size() {
		return scoreQueue.size();
	}
	
	public Collection<Double> keySet() {
		return scoreQueue.keySet();
	}

	public List<String> get(Double key) {
		return scoreQueue.get(key);
	}	
	
	public boolean remove(double key, String value) {
		if (scoreQueue.remove(key, value)) {
			numDeletes++;
			return true;
		}
		return false;
	}
	
	public void removeAll(Double key) {
		numDeletes += scoreQueue.get(key).size();
		scoreQueue.removeAll(key);	
	}
	
	public Multiset<Double> keys() {
		return scoreQueue.keys();
	} 	
	
	public Collection<String> values() {
		return scoreQueue.values();
	} 
	
	public Double getLowestScore() {
		ArrayList<Double> listKeys = new ArrayList<Double>(scoreQueue.keySet());
		return listKeys.get(listKeys.size()-1);
	}

	public int getNumInserts() {
		return numInserts;
	}

	public int getNumDeletes() {
		return numDeletes;
	}

	public int getCapacity() {
		return capacity;
	}

}
