package eu.smartdatalake.simsearch.csv.numerical;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
//import java.util.concurrent.TimeUnit;

import eu.smartdatalake.simsearch.ISimSearch;
import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.PartialResult;
import eu.smartdatalake.simsearch.measure.ISimilarity;

/**
 * Implements similarity search against numerical values indexed in a B+-tree.
 * @param <K>  Type variable representing the keys of the indexed objects.
 * @param <V>  Type variable representing the values of the indexed objects.
 */
public class NumericalSimSearch<K extends Comparable<? super K>, V> implements ISimSearch<K, V> {

	Logger log = null;
	
	BPlusTree<K, V> index;   //The underlying B+-tree index
	
	private K searchKey; // Key to calculate similarities with
	private K leftKey; // Key currently visited leftwards at the leaf level
	private K rightKey; // Key currently visited rightwards at the leaf
						// level
	private int i; // Number of results returned so far
//	private int topk; // Number of results requested by the query
//	private double domainKeyRange; // The range of keys used as denominator in
								// similarity calculations

	ISimilarity<K> numSimilarity;
//	public K minKey;
//	public K maxKey;
	
	Double simLeft;
	Double simRight;
	Double simScore;
	
	ConcurrentLinkedQueue<PartialResult> partialResults;

	/**
	 * Constructor
	 * @param index  The underlying B+-tree index to be used in the search.
	 * @param key  The search key.
	 * @param k   Number of results requested by the query.
	 * @param simMeasure  The similarity measure to be used.
	 * @param logStream  Handle to the log file for notifications and execution statistics.
	 */
	public NumericalSimSearch(BPlusTree<K, V> index, K key, ISimilarity<K> simMeasure, ConcurrentLinkedQueue<PartialResult> partialResults, Logger log) {
		
		this.log = log;
		this.partialResults = partialResults;
		this.index = index;

		searchKey = leftKey = rightKey = key;
//		topk = k;    // FIXME: May be not used, in order to provide progressive results
		i = 0;
	
		// Instantiate the numerical similarity function
		this.numSimilarity = simMeasure;	
	}


	/**
	 * Provides the ranking of the most recently issued result
	 * @return  The ranking order.
	 */
	public int getRank() {
		return i;
	}


	/**
	 * Provides the similarity score of the most recently issued result
	 * @return  The computed similarity score.
	 */
	public Double getScore() {
		return simScore;
	}
	

	/**
	 * Progressively provides the next query result either leftwards or rightwards in the key range depending on similarity scores.
	 */
	@Override
	public List<V> getNextResult() {
	
		List<V> results = new ArrayList<V>();

		// Initially, find out whether search key exists, 
		// as this must give  exact results
		if ((i == 0) && (leftKey.equals(rightKey))) {
		
			// If search key exists in the index, return its contents
			results = index.search(searchKey); 

			// Estimate similarities with next keys to be visited leftwards
			// and rightwards
			simLeft = calcSimLeft(searchKey);
			simRight = calcSimRight(searchKey);

			// If exact key is found, assign a similarity score of 1.0
			if (results != null) { 
				simScore = 1.0;
				i++;
				return results;
			}
		}

		// Termination criterion: no more keys available for searching
		if ((simLeft == 0.0) && (simRight == 0.0)) {
//			System.out.println("FINISHED searching. Results collected from " + i + " keys. Final LEFT score:"
//					+ simLeft + " RIGHT score:" + simRight);
			return null;
		}
		
		// As long as keys are not exhausted and we are above the threshold
		// OR top-k is not yet reached
//		if (i < topk) {
			if (simLeft > simRight) { 	// Continue LEFT-wards
				simScore = simLeft;
				results = index.search(leftKey);
				simLeft = calcSimLeft(searchKey);
			} else { 					// Continue RIGHT-wards
				simScore = simRight;
				results = index.search(rightKey);
				simRight = calcSimRight(searchKey);
			}
			i++;
//		}

		return results;
	}

	
	/**
	 * Inserts to the result queue a batch of qualifying results corresponding to a single key.
	 * @return  The number of qualifying results having the same key.
	 */
	public int fetchNextBatch() {
		
		int n = i;
		
		List<V> results = new ArrayList<V>();

		// Initially, find out whether search key exists, 
		// as this must give exact results
		if ((i == 0) && (leftKey.equals(rightKey))) {
		
			// If search key exists in the index, return its contents
			results = index.search(searchKey); 

			// Estimate similarities with next keys to be visited leftwards and rightwards
			simLeft = calcSimLeft(searchKey);
			simRight = calcSimRight(searchKey);

			// If exact key is found, assign a similarity score of 1.0
			for (V v: results) {
				simScore = 1.0;
				partialResults.add(new PartialResult(v, searchKey, simScore));
//				System.out.println("key:"+ searchKey + " val:" + v);
				i++;
			}
		}

		// Termination criterion: no more keys available for searching
		if ((simLeft == 0.0) && (simRight == 0.0)) {
//			System.out.println("FINISHED searching. Results collected from " + i + " keys. Final LEFT score:"
//					+ simLeft + " RIGHT score:" + simRight);
			return 0;
		}
		
		// As long as keys are not exhausted and we are above the threshold
		// OR top-k is not yet reached
//		if (i < topk) {
			if (simLeft > simRight) { 	// Continue LEFT-wards
				simScore = simLeft;
				results = index.search(leftKey);
				for (V v: results) {
					partialResults.add(new PartialResult(v, leftKey, simScore));
//					System.out.println("key:"+ leftKey + " val:" + v);
					i++;
				}
				simLeft = calcSimLeft(searchKey);
			} else { 					// Continue RIGHT-wards
				simScore = simRight;
				results = index.search(rightKey);
				for (V v: results) {
					partialResults.add(new PartialResult(v, rightKey, simScore));
//					System.out.println("key:"+ rightKey + " val:" + v);
					i++;
				}
				simRight = calcSimRight(searchKey);
			}
			
//		}

		return (i - n);   // The number of items added in this batch
	}

	
	/**
	 * Gets the next available key LEFT-wards and calculates a similarity score.
	 * @param key  The search key.
	 * @return  Similarity score of the found key with the search key.
	 */
	private Double calcSimLeft(K key) {

		K curLeftKey = index.getRoot().nextKeyLeftwards(leftKey);
		if (curLeftKey != null) {
			leftKey = curLeftKey;
			return numSimilarity.calc(key, leftKey);
		} else
			return 0.0;
	}


	/**
	 * Gets the next available key RIGHT-wards and calculates a similarity score.
	 * @param key  The search key.
	 * @return  Similarity score of the found key with the search key.
	 */
	private Double calcSimRight(K key) {

		K curRightKey = index.getRoot().nextKeyRightwards(rightKey);
		if (curRightKey != null) {
			rightKey = curRightKey;
			return numSimilarity.calc(key, rightKey);
		} else
			return 0.0;
	}


	/**
	 * Collects the specified number of results of a similarity search query.
	 * @param k  The number of top-k results to fetch.
	 * @param results  A collection with the query results.
	 * @return  The number of collected results.
	 */
	public long compute(int k) {
		
		int n = 0;
		try {
			while (partialResults.size() < k) {
				n += fetchNextBatch();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return n;
	}
	
}

