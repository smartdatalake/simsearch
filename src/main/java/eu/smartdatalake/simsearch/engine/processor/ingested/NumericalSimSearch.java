package eu.smartdatalake.simsearch.engine.processor.ingested;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
//import java.util.concurrent.TimeUnit;

import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.engine.measure.ISimilarity;
import eu.smartdatalake.simsearch.engine.processor.ISimSearch;
import eu.smartdatalake.simsearch.engine.processor.ranking.PartialResult;
import eu.smartdatalake.simsearch.engine.processor.ranking.RankedList;
import eu.smartdatalake.simsearch.manager.ingested.numerical.BPlusTree;

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
	private int numMatches; // Number of results returned so far
//	private int topk; // Number of results requested by the query
//	private double domainKeyRange; // The range of keys used as denominator in
								// similarity calculations

	ISimilarity<K> numSimilarity;
//	public K minKey;
//	public K maxKey;
	
	Double simLeft, simRight, simScore;
	Double distLeft, distRight, dist;
	
	RankedList partialResults;

	/**
	 * Constructor
	 * @param index  The underlying B+-tree index to be used in the search.
	 * @param key  The search key.
	 * @param simMeasure  The similarity measure to be used.
	 * @param partialResults  Queue that collects the results of this search.
	 * @param log  Handle to the log file for notifications and execution statistics.
	 */
	public NumericalSimSearch(BPlusTree<K, V> index, K key, ISimilarity<K> simMeasure, RankedList partialResults, Logger log) {
		
		this.log = log;
		this.partialResults = partialResults;
		this.index = index;

		searchKey = leftKey = rightKey = key;
//		topk = k;    // FIXME: May be not used, in order to provide progressive results
		numMatches = 0;
	
		// Instantiate the numerical similarity function
		this.numSimilarity = simMeasure;
	}


	/**
	 * Provides the ranking of the most recently issued result.
	 * @return  The ranking order.
	 */
	public int getRank() {
		return numMatches;
	}


	/**
	 * Provides the similarity score of the most recently issued result.
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

		// Initially, find out whether search key exists, as this must give  exact results
		if ((numMatches == 0) && (leftKey.equals(rightKey))) {
		
			// If search key exists in the index, return its contents
			results = index.search(searchKey); 

			// Estimate similarities with next keys to be visited leftwards and rightwards
			simLeft = calcSimLeft(searchKey);
			simRight = calcSimRight(searchKey);

			// If exact key is found, assign a similarity score of 1.0
			if (results != null) { 
				simScore = 1.0;
				numMatches++;
				return results;
			}
		}

		// Termination criterion: no more keys available for searching
		if ((leftKey == null) && (rightKey == null)) {
//			System.out.println("FINISHED searching. Results collected from " + numMatches + " keys. Final LEFT score:"
//					+ simLeft + " RIGHT score:" + simRight);
			return null;
		}
		
		// As long as keys are not exhausted and we are above the threshold
		// OR top-k is not yet reached
//		if (numMatches < topk) {
			if (simLeft > simRight) { 	// Continue LEFT-wards
				simScore = simLeft;
				results = index.search(leftKey);
				simLeft = calcSimLeft(searchKey);
			} else { 					// Continue RIGHT-wards
				simScore = simRight;
				results = index.search(rightKey);
				simRight = calcSimRight(searchKey);
			}
			numMatches++;
//		}

		return results;
	}

	
	/**
	 * Inserts to the result queue a batch of qualifying results corresponding to a single key.
	 * @return  The number of qualifying results in the next batch.
	 */
	public int fetchNextBatch() {
		
		int n = numMatches;
		
		List<V> results = new ArrayList<V>();

		// Initially, find out whether search key exists, as this must give exact results
		if ((numMatches == 0) && (leftKey.equals(rightKey))) {
		
			// If search key exists in the index, return its contents
			results = index.search(searchKey); 

			// Estimate similarities with next keys to be visited leftwards and rightwards
			simLeft = calcSimLeft(searchKey);
			simRight = calcSimRight(searchKey);

			// If exact key is found, assign a similarity score of 1.0
			if (results != null) {
				for (V v: results) {
					simScore = 1.0;
					partialResults.add(new PartialResult(v, searchKey, simScore));
//					System.out.println("key:"+ searchKey + " val:" + v);
					numMatches++;
				}
			}
		}

		// Termination criterion: no more keys available for searching
		if ((leftKey == null) && (rightKey == null)) {
//			System.out.println("FINISHED searching. Results collected from " + numMatches + " keys. Final LEFT score:"
//					+ simLeft + " RIGHT score:" + simRight);
			return -1;   // CAUTION: Special value to indicate that tree traversal is exhausted
		}		
		
		// As long as keys are not exhausted and we are above the threshold
		// OR top-k is not yet reached
//		if (i < topk) {
			if ((simLeft > simRight) || (rightKey == null)) { 	// Continue LEFT-wards
				simScore = simLeft;
				results = index.search(leftKey);
				if (results != null) {
					for (V v: results) {
						partialResults.add(new PartialResult(v, leftKey, simScore));
//						System.out.println("LEFT key:"+ leftKey + " val:" + v);
						numMatches++;
					}
					simLeft = calcSimLeft(searchKey);
				}		
			} else if ((simLeft <= simRight) || (leftKey == null)) { // Continue RIGHT-wards
				simScore = simRight;
				results = index.search(rightKey);
				if (results != null) {
					for (V v: results) {
						partialResults.add(new PartialResult(v, rightKey, simScore));
//						System.out.println("RIGHT key:"+ rightKey + " val:" + v);
						numMatches++;
					}
					simRight = calcSimRight(searchKey);
				}
			}
			
//		}
		
		return (numMatches - n);   // The number of items added in this batch
	}


	/**
	 * Provides the first batch of (at least topk) results and also specifies the scaling factor for scoring.
	 * CAUTION! Results are obtained with increasing distance; scores will be assigned once the scale factor is fixed.
	 * @param topk  The number of the final top-k results.
	 * @return  The number of items added in the first batch.
	 */
	public int fetchFirstBatch(int topk) {
		
		RankedList topkResults = new RankedList();
		PartialResult pRes;
		
		while (topkResults.size() < topk) {
			
		List<V> results = new ArrayList<V>();

		// Initially, find out whether search key exists, as this must give exact results
		if ((numMatches == 0) && (leftKey.equals(rightKey))) {
		
			// If search key exists in the index, return its contents
			results = index.search(searchKey); 

			// Estimate distances (NOT scores) with next keys to be visited leftwards and rightwards
			distLeft = calcDistanceLeft(searchKey);
			distRight = calcDistanceRight(searchKey);

			// If exact key is found, assign a similarity score of 1.0
			if (results != null) {
				for (V v: results) {
					dist = 0.0;   // Distance, NOT score!
					topkResults.add(new PartialResult(v, searchKey, dist));
//					System.out.println("key:"+ searchKey + " val:" + v);
					numMatches++;
				}
			}
		}

		// Update scale factor
		if ((numMatches >= topk) && !numSimilarity.isScaleSet())
			numSimilarity.setScaleFactor(dist);
		
		
		// Termination criterion: no more keys available for searching
		if ((leftKey == null) && (rightKey == null)) {
//			System.out.println("FINISHED searching. Results collected from " + numMatches + " keys. LEFT distance:"
//					+ distLeft + " RIGHT distance:" + distRight);
			return -1;   // CAUTION: Special value to indicate that tree traversal is exhausted
		}
	
		// As long as keys are not exhausted and we are above the threshold
		// OR top-k is not yet reached
//		if (i < topk) {
			if ((distLeft < distRight) || (rightKey == null)) { 	// Continue LEFT-wards
				dist = distLeft;
				results = index.search(leftKey);
				if (results != null) {
					for (V v: results) {
						topkResults.add(new PartialResult(v, leftKey, dist));
//						System.out.println("LEFT key:"+ leftKey + " val:" + v);
						numMatches++;
					}
					distLeft = calcDistanceLeft(searchKey);
				}			
			} else if ((distLeft >= distRight) || (leftKey == null)) { 	// Continue RIGHT-wards
				dist = distRight;
				results = index.search(rightKey);
				if (results != null) {
					for (V v: results) {
						topkResults.add(new PartialResult(v, rightKey, dist));
//						System.out.println("RIGHT key:"+ rightKey + " val:" + v);
						numMatches++;
					}
					distRight = calcDistanceRight(searchKey);
				}	
			}
			
			// Update scale factor
			if ((numMatches >= topk) && !numSimilarity.isScaleSet())
				numSimilarity.setScaleFactor(dist);
//		}

		}	
	
		// Once the scale factor is fixed, all results can be issued to the priority queue
		// Put previously collected results into the priority queue ...
		// ... with a score according to exponential decay function
		Iterator<PartialResult> qIter = topkResults.iterator();
		while (qIter.hasNext()) { 
			pRes = qIter.next();
			pRes.setScore(numSimilarity.scoring(pRes.getScore()));
			partialResults.add(pRes);
		}
		
		// CAUTION! Must update similarity scores on left and right pointers
		simLeft = numSimilarity.calc(searchKey, leftKey);
		simRight = numSimilarity.calc(searchKey, rightKey);
		
		return numMatches;   // The number of items added in the first batch
	}

	
	/**
	 * Gets the next available key LEFT-wards and calculates a similarity score.
	 * @param key  The search key.
	 * @return  Similarity score of the found key with the search key.
	 */
	private Double calcSimLeft(K key) {

		leftKey = index.getRoot().nextKeyLeftwards(leftKey);
		if (leftKey != null)
			return numSimilarity.calc(key, leftKey);
		else
			return 0.0;
	}

	/**
	 * Gets the next available key LEFT-wards and calculates its distance.
	 * @param key  The search key.
	 * @return Distance of the found key from the search key.
	 */
	private Double calcDistanceLeft(K key) {

		leftKey = index.getRoot().nextKeyLeftwards(leftKey);
		if (leftKey != null)
			return numSimilarity.getDistanceMeasure().calc(key, leftKey);
		else
			return Double.POSITIVE_INFINITY;
	}
	
	/**
	 * Gets the next available key RIGHT-wards and calculates a similarity score.
	 * @param key  The search key.
	 * @return  Similarity score of the found key with the search key.
	 */
	private Double calcSimRight(K key) {

		rightKey = index.getRoot().nextKeyRightwards(rightKey);
		if (rightKey != null)
			return numSimilarity.calc(key, rightKey);
		else
			return 0.0;
	}

	/**
	 * Gets the next available key RIGHT-wards and calculates its distance.
	 * @param key  The search key.
	 * @return Distance of the found key from the search key.
	 */
	private Double calcDistanceRight(K key) {

		rightKey = index.getRoot().nextKeyRightwards(rightKey);
		if (rightKey != null)
			return numSimilarity.getDistanceMeasure().calc(key, rightKey);
		else
			return Double.NEGATIVE_INFINITY;
	}
	
	/**
	 * Collects the specified number of results of a similarity search query.
	 * @param topk  The number of the final top-k results.
	 * @param M  The number of top-scoring results to fetch and populate the ranked list.
	 * @return  The number of collected results.
	 */
	public long compute(int topk, int M) {
		
		int n = 0;
		int m = 0;
		try {
			// First, get a batch of at least top-k results in order to specify the scale factor for scoring 
			if ((n = fetchFirstBatch(topk)) < 0)  // No matches
				return 0;
			// Then, continue with more results assigning scores
			while (partialResults.size() < M) {
				m = fetchNextBatch();
				if (m >= 0)
					n += m;
				else         // Tree is exhausted
					break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return n;
	}
	
}

