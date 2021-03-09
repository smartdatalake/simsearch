package eu.smartdatalake.simsearch.csv.spatial;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.engine.ISimSearch;
import eu.smartdatalake.simsearch.engine.PartialResult;
import eu.smartdatalake.simsearch.measure.ISimilarity;


/**
 * Implements similarity search against geometry locations indexed in a R-tree.
 * @param <K>  Type variable representing the datasetIdentifiers of the indexed objects.
 * @param <V>  Type variable representing the locations of the indexed objects.
 */
public class kNNSearch<K extends Comparable<? super K>, V> implements ISimSearch<K, V> {

	Logger log = null;
	
	RTree<K, V> index;   //The underlying R-tree index
	
	ISimilarity<V> locSimilarity;
	
	ListMultimap<Double, Location> matches;
	Iterator<Double> iterMatches;
	private Double score;
	ConcurrentLinkedQueue<PartialResult> partialResults;
	
	private V searchLoc; // The search location
	
	/**
	 * Constructor
	 * @param index  The underlying R-tree index to be used in the search.
	 * @param loc  Query location specified for this spatial similarity search.
	 * @param simMeasure  The similarity measure to be used.
	 * @param partialResults   The queue to collect query results.
	 * @param log  Handle to the log file for notifications and execution statistics.
	 */
	public kNNSearch(RTree<K, V> index, V loc, ISimilarity<V> simMeasure, ConcurrentLinkedQueue<PartialResult> partialResults, Logger log) {
		
		this.log = log;
		this.index = index;
		this.searchLoc = loc;
		this.locSimilarity = simMeasure;
		this.partialResults = partialResults;
		this.score = null;
	}


	/**
	 * Provides the similarity score of the most recently issued result
	 * @return  The computed similarity score.
	 */
	public Double getScore() {
		
		return score;
	}

	@Override
	//TODO : Remove this unused method!
	public List<V> getNextResult() {
		
		// Consume results by ascending distance value
		while (iterMatches.hasNext()) {
			double distance = iterMatches.next(); 
			score = this.locSimilarity.scoring(distance);
			
//			System.out.println("SCORE: " + score + " #results: " + matches.get(distance).size());
			return (List<V>) matches.get(distance);
		}
		
		return null;
	}

	/**
	 * Inserts to the result queue a batch of qualifying results corresponding to a single key.
	 * @return  The number of qualifying results having the same key.
	 */
	public int fetchNextBatch() {
		
		int n = 0;
		// Consume results by ascending distance value
		while (iterMatches.hasNext()) {
			double distance = iterMatches.next(); 
			score = this.locSimilarity.scoring(distance);
			// Multiple objects may be at the same distance
			for (Location p : matches.get(distance)) {
				partialResults.add(new PartialResult(p.key, p.loc, score));
				n++;
			}
		}
		return n;
	}
	
	/**
	 * Computes the k-NN results.
	 * FIXME: Process does not actually work progressively; it currently collects all k-NN results before issuing them.
	 * @param k  The number of results to fetch, i.e., those with the top-k (closest) distances to the query point.
	 * @return  The number of collected results.
	 */
	public long compute(int k) { 

		Object[] res = index.getKNearestNeighbors(searchLoc, k);
		
		// Results are sorted by ascending distance from the query location
		matches = Multimaps.newListMultimap(new TreeMap<>(), ArrayList::new);

		double distance;
		Location p;
		
		// Calculate distances for the retrieved locations
		for (int i = 0; i < res.length; i++) {
			p = (Location) res[i];
			distance = this.locSimilarity.getDistanceMeasure().calc((V) p.loc);
			matches.put(distance, p);
		}

		// Set the iterator on the keys (i.e., distances) of the collected results
		iterMatches = matches.keySet().iterator(); 

		return res.length;
	}
	
}
