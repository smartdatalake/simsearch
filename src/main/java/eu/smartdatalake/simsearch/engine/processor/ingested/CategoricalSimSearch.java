package eu.smartdatalake.simsearch.engine.processor.ingested;

import gnu.trove.list.TDoubleList;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.engine.measure.ISimilarity;
import eu.smartdatalake.simsearch.engine.processor.ISimSearch;
import eu.smartdatalake.simsearch.engine.processor.ranking.PartialResult;
import eu.smartdatalake.simsearch.engine.processor.ranking.RankedList;
import eu.smartdatalake.simsearch.manager.ingested.categorical.IntSetCollection;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * Implements kNN similarity search against a collection of sets of tokens indexed by an inverted index.
 * Processing logic is similar to the one in All Pairs algorithm for set similarity joins
 * @param <K>  Type variable representing the keys of the indexed objects.
 * @param <V>  Type variable representing the values (sets of tokens) of the indexed objects.
 */
public class CategoricalSimSearch<K extends Comparable<? super K>, V> implements ISimSearch<K, V> {

	Logger log = null;
	TIntList[] idx;
	TIntList matches;
	TDoubleList matchScores;
	List<String> matchKeys;
	ISimilarity simMeasure;
	int num;   // Serial number of the most recently issued result of this query


	/**
	 * Constructor
	 * @param idx  The inverted indexed built over the input sets of tokens. 
	 * @param simMeasure  The similarity measure to be used in the search.
	 * @param log  Handle to the log file for notifications and execution statistics.
	 */
	public CategoricalSimSearch(TIntList[] idx, ISimilarity simMeasure, Logger log) {
		
		this.idx = idx;
		this.simMeasure = simMeasure;
		this.log = log;	
	}
	
	/**
	 * Progressively provides the next query result.
	 * NOT applicable with this type of search, as results are issued directly to the queue.
	 */
	@Override
	public List<V> getNextResult() {
/*		
		num++;
		if (num < matchKeys.size()) {
			List<String> res =  new ArrayList<String>();
			res.add(matchKeys.get(num));	
			return (List<V>) res;
		}
*/		
		return null;
	}
	

	/**
	 * Provides the similarity score of the most recently issued result
	 * @return  The computed similarity score.
	 */
/*	public Double getScore() {
		
		return matchScores.get(num);
	}
*/	
	
	/**
	 * Progressively collects the top-k most similar results.
	 * Once the score of a result is above the estimated score of any unseen items, this result is issued.
	 * @param queryCollection  The (transformed) collection of tokens specified by the query.
	 * @param targetCollection  The (transformed) collection of the sets of tokens in the dataset.
	 * @param origTokenSetCollection  The original collection of the sets of tokens in the dataset.
	 * @param topk  The number of the final top-k results.
	 * @param M  The number of top-scoring results to fetch and populate the ranked list.
	 * @param partialResults  Queue to collect query results.
	 * @return  The number of collected results.
	 */
	public int compute(IntSetCollection queryCollection, IntSetCollection targetCollection, Map<?, ?> origTokenSetCollection, int topk, int M, RankedList partialResults) {  //OutputHandler oh

		RankedList topkResults =  new RankedList();
		PartialResult pRes;
		
		num = -1;    //no matches found yet
		matchKeys = new ArrayList<String>();
		
		// Initializations
		int numMatches = 0;

		int minLength = 0, maxLength = 0, candidate, eqoverlap, rPrefixLength, sPrefixLength, prefixBound, i, place;
		int[] minOverlap = null, prefixLength;
		TIntSet candidates;
		boolean found;
		double sim, simThreshold, ratio;
		String key = null;

		matches = new TIntArrayList();
		matchScores = new TDoubleArrayList();	
		
//		int count = 0;
		int[] r = queryCollection.sets[0];     //Just one probing set, that the query
	
		// Initialize
		simThreshold = 0.0;
		eqoverlap = 1;
		prefixBound = r.length;
		candidates = new TIntHashSet();
		i = 0;

		int j;
		int pos, step, end, diff_front, diff_rear;
		int start = 0;

		while (i < prefixBound) {

			// skip this token if not in the index
			if (r[i] < 0 || r[i] >= idx.length || idx[r[i]].size() == 0) {
				i++;
				continue;
			}

			// Calculate differences in length against the indexed items to determine the search order
			diff_front = r.length - targetCollection.sets[idx[r[i]].get(0)].length;
			diff_rear = r.length - targetCollection.sets[idx[r[i]].get(idx[r[i]].size() - 1)].length;

			if ((diff_front > 0) || (diff_rear < 0)) {
				// Binary search
				end = idx[r[i]].size() - 1;
				while (start < end) {
					pos = (int) Math.floor((start + end) / 2.0);
					if (targetCollection.sets[start].length < r.length) {
						start = pos + 1; // search on the right part
					} else {
						end = pos - 1; // search on the left part
					}
				}
				// To start searching for candidates in ascending order of length
				end = idx[r[i]].size();
				step = 1;
				j = start;

			} else {
				diff_front = (diff_front > 0) ? 0 : Math.abs(diff_front);
				diff_rear = (diff_rear < 0) ? Math.abs(diff_rear) : 0;

				if (diff_front < diff_rear) {
					// Examine indexed items in ascending order of length
					j = 0;
					end = idx[r[i]].size();
					step = 1;
				} else {
					// Examine indexed items in descending order of length
					j = idx[r[i]].size() - 1;
					end = -1;
					step = -1;
				}
			}

			// Search for candidates using the index
			while (step * (end - j) > 0) {
				candidate = idx[r[i]].get(j);

				j += step;
				// Reverse order of search (descending) for candidates in the next iteration
				if ((j > 0) && (j == end)) {
					j = start - 1;
					end = -1;
					step = -1;
				}

				// Apply length filter and set eqoverlap, depending on the
				// (ascending/descending) order of search
				if (simThreshold > 0) {
					if (targetCollection.sets[candidate].length < minLength) {
						if (step == 1)
							continue;
						else
							break;
					}
					if (targetCollection.sets[candidate].length > maxLength - i) {
						if (step == 1)
							break;
						else
							continue;
					}
					eqoverlap = minOverlap[targetCollection.sets[candidate].length - minLength];
				}

				// Apply prefix filter
				rPrefixLength = r.length - eqoverlap + 1;
				if (rPrefixLength < i) {
					continue;
				}

				sPrefixLength = targetCollection.sets[candidate].length - eqoverlap + 1;
				found = false;
				for (int m = 0; m < sPrefixLength; m++) {
					if (targetCollection.sets[candidate][m] == r[i]) {
//						System.out.println("Found candidate " + targetCollection.keys[candidate]);
						found = true;
						break;
					}
				}

				if (found) {

					// Skip examination of already seen candidates
					// Exclude identity from kNN results for self-join
					if (candidates.contains(candidate))  {   //|| (selfJoin && count == candidate)
						continue;
					}

					candidates.add(candidate);

					// Verify candidate
					// CAUTION! Final similarity score must NOT be calculated according to the exponential decay function!
					sim = verifyWithScore(r, targetCollection.sets[candidate]);
//					System.out.println( candidate + " " + sim);

					// Update items and scores in the list of results
					if (matches.size() < M || sim > simThreshold) {

						// Find the place in the lists where to add
						// the new score (and the corresponding item)
						place = matchScores.size() - 1;
						while ((place >= 0) && (matchScores.get(place) < sim)) {
							--place;
						}
						place++;

						// Add new score and item to the lists
						if (place < M) {
							matchScores.insert(place, sim);
							matches.insert(place, candidate);

							// Expel superfluous item and score from the list
							if (matchScores.size() > M) {
								matchScores.removeAt(M);
								matches.removeAt(M);
							}
						}

						// Adjust threshold
						if (matchScores.size() >= M) {

							simThreshold = matchScores.get(matchScores.size() - 1);

							ratio = simThreshold / (1 + simThreshold);

							// Recompute bounds for prefix filtering based on the updated threshold
							minLength = (int) Math.ceil(r.length * simThreshold);
							maxLength = (int) Math.ceil(r.length / simThreshold);

							if (maxLength - minLength + 1 > 0) {
								minOverlap = new int[maxLength - minLength + 1];
								prefixLength = new int[maxLength - minLength + 1];

								for (int p = 0; p < minOverlap.length; p++) {
									minOverlap[p] = (int) Math.ceil(
											Math.round(ratio * (r.length + minLength + p) * 100000) / 100000.0);
									prefixLength[p] = r.length - minOverlap[p] + 1;
								}

								prefixBound = prefixLength[0];
							}
						}
					}
				}
			}
			i++;
			
			// Update the similarity upper bound of any future matches
			double scoreUpperBound = 1 - (i / (1.0 * r.length));
//			System.out.println("distance: " +  i / (1.0 * r.length) + " scoreUpperBound: " + scoreUpperBound);

			// Check if any results can be issued directly to the queue
			while (matchScores.size() > numMatches && matchScores.get(numMatches) >= scoreUpperBound) {
				key = targetCollection.keys[matches.get(numMatches)];   // Result identifier
				matchKeys.add(key);
				pRes = new PartialResult(key, origTokenSetCollection.get(key), 1-matchScores.get(numMatches));
				
				numMatches++;
				
				// While less than topk results are fetched, put them in a temporary list
				if (numMatches < topk) {
					topkResults.add(pRes);
				}
				else if (numMatches == topk) {
					topkResults.add(pRes);
					// Set the scale factor to be used in scoring as the k-th distance
					simMeasure.setScaleFactor(pRes.getScore());
					// Put previously collected results into the priority queue ...
					// ... with a score according to exponential decay function
					Iterator<PartialResult> qIter = topkResults.iterator();
					while (qIter.hasNext()) { 
						pRes = qIter.next();
						pRes.setScore(simMeasure.scoring(pRes.getScore()));
						partialResults.add(pRes);
					}
				}
				else {  
					// After the k-th result, the scale factor has been set, so issue them directly ...
					// ... with a score according to exponential decay function
					pRes.setScore(simMeasure.scoring(pRes.getScore()));
					partialResults.add(pRes);
				}     
			}		
		}

		return numMatches;
	}

	
	/**
	 * Returns the similarity score between two sets of tokens used in the verification stage of similarity search.
	 * @param r  Array of datasetIdentifiers for the first set.
	 * @param s  Array of datasetIdentifiers for the second set.
	 * @return  The Jaccard similarity between the two input sets.
	 */
	private double verifyWithScore(int[] r, int[] s) {
		
		int olap, pr, ps, maxr, maxs;

		olap = 0;
		pr = 0;
		ps = 0;
		maxr = r.length - pr + olap;
		maxs = s.length - ps + olap;

		while (maxr > olap && maxs > olap) {

			if (r[pr] == s[ps]) {
				pr++;
				ps++;
				olap++;
			} else if (r[pr] < s[ps]) {
				pr++;
				maxr--;
			} else {
				ps++;
				maxs--;
			}
		}
		
		// Scaling of the calculated similarity score
		return (double) (olap / (1.0 * (r.length + s.length - olap)));
	}
	
}