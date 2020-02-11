package eu.smartdatalake.simsearch.numerical;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import eu.smartdatalake.simsearch.ISimSearch;
import eu.smartdatalake.simsearch.ISimilarity;

/**
 * Implements similarity search against numerical values indexed in a B+-tree.
 * @param <K>  Type variable representing the keys of the indexed objects.
 * @param <V>  Type variable representing the values of the indexed objects.
 */
public class NumericalSimSearch<K extends Comparable<? super K>, V> implements ISimSearch<K, V> {

	PrintStream logStream = null;
	
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

	/**
	 * Constructor
	 * @param index  The underlying B+-tree index to be used in the search.
	 * @param key  The search key.
	 * @param simMeasure  The similarity measure to be used.
	 * @param logStream  Handle to the log file for notifications and execution statistics.
	 */
	public NumericalSimSearch(BPlusTree<K, V> index, K key, ISimilarity<K> simMeasure, PrintStream logStream) {
		
		this.logStream = logStream;
		this.index = index;
//		minKey = index.calcMinKey();
//		maxKey = index.calcMaxKey();
		searchKey = leftKey = rightKey = key;
//		topk = k;    //FIXME: Currently not used, in order to provide progressive results
		i = 0;
/*
		// The range of keys used as denominator in all similarity score calculations
		double domainKeyRange = (key.compareTo(minKey) < 0)
				? (Double.parseDouble(maxKey.toString()) - Double.parseDouble(key.toString()))
				: ((key.compareTo(maxKey) > 0)
						? (Double.parseDouble(key.toString()) - Double.parseDouble(minKey.toString()))
						: (Double.parseDouble(maxKey.toString()) - Double.parseDouble(minKey.toString())));
*/		
		// Instantiate the numerical similarity function
//		numSimilarity =  new ProportionalSimilarity<K>(domainKeyRange);
		this.numSimilarity = simMeasure;
//		System.out.println("MinKey: " + minKey + ", MaxKey: " + maxKey + " , domainRange: " + domainKeyRange);		
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
	
/*
	// Provides the range of indexed keys 
	public double getDomainKeyRange() {
		return domainKeyRange;
	}
*/	

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

/*
	// Returns the similarity score in [0..1] between two NUMERIC keys
	// FIXED: Also returns correct similarities if keys are beyond the range
	// indexed in the tree
	private double simScoreDouble(K k1, K k2) {
		
		return 1.0
				- (Math.abs((Double.parseDouble(k1.toString()) - Double.parseDouble(k2.toString()))) / domainKeyRange);
	}
*/
	
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

/*
	@Override
	public void compute(ConcurrentLinkedQueue<Result> results) {
		
		List<V> qryAnswer;
		double score;
		Result res;
		try {
			while (true) {
				TimeUnit.NANOSECONDS.sleep(100);
				qryAnswer = getNextResult();
				if (qryAnswer != null) {
					score = getScore();
					for (V val : qryAnswer) {
						res = new Result(val.toString(), score);
						results.add(res);
					}
				} else // No more results available from this query
					break;
			}
		} catch (Exception e) { // InterruptedException
			e.printStackTrace();
		}	
	}
*/	
	
}

