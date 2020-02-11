package eu.smartdatalake.simsearch.categorical;

import gnu.trove.list.TDoubleList;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import eu.smartdatalake.simsearch.ISimSearch;
import eu.smartdatalake.simsearch.Result;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * Implements kNN similarity search against a collection of sets of tokens indexed by an inverted index.
 * Processing logic is similar to the one in All Pairs algorithm for set similarity joins
 * @param <K>  Type variable representing the keys of the indexed objects.
 * @param <V>  Type variable representing the values (sets of tokens) of the indexed objects.
 */
public class CategoricalSimSearch<K extends Comparable<? super K>, V> implements ISimSearch<K, V> {

	PrintStream logStream = null;
	TIntList[] idx;
	TIntList matches;
	TDoubleList matchScores;
	List<String> matchKeys;
	int num;   // Serial number of the most recently issued result of this query


	/**
	 * Constructor
	 * @param idx  The inverted indexed built over the input sets of tokens. 
	 * @param logStream  Handle to the log file for notifications and execution statistics.
	 */
	public CategoricalSimSearch(TIntList[] idx, PrintStream logStream) {
		
		this.idx = idx;
		this.logStream = logStream;		
	}
	
	/**
	 * Progressively provides the next query result.
	 */
	@Override
	public List<V> getNextResult() {
		
		num++;
		if (num < matchKeys.size()) {
			List<String> res =  new ArrayList<String>();
			res.add(matchKeys.get(num));	
			return (List<V>) res;
		}
		
		return null;
	}
	

	/**
	 * Provides the similarity score of the most recently issued result
	 * @return  The computed similarity score.
	 */
	public Double getScore() {
		
		return matchScores.get(num);
	}
	
	
	/**
	 * Computes the top-k most similar results.
	 * FIXME: Process does not actually work progressively; it must collect the top-k most similar results before issuing them
	 * @param queryCollection  The (transformed) collection of tokens specified by the query.
	 * @param targetCollection  The (transformed) collection of the sets of tokens in the dataset.
	 * @param k  The number of results to fetch, i.e., those with the top-k (highest) similarity scores.
	 * @param results  Queue to collect query results.
	 * @return  The number of collected results.
	 */
	public long compute(IntSetCollection queryCollection, IntSetCollection targetCollection, int k, ConcurrentLinkedQueue<Result> results) {  //OutputHandler oh

		num = -1;    //no matches found yet
		matchKeys = new ArrayList<String>();
		
		// Initializations
		Verification verification = new Verification();

		long numMatches = 0;

		int minLength = 0, maxLength = 0, candidate, eqoverlap, rPrefixLength, sPrefixLength, prefixBound, i, place;
		int[] minOverlap = null, prefixLength;
		TIntSet candidates;
		boolean found;
		double sim, simThreshold, ratio;

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

			// Calculate differences in length against the indexed items to determine the
			// search order
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
					sim = verification.verifyWithScore(r, targetCollection.sets[candidate]);
//					System.out.println(count + " " + candidate + " " + sim);

					// Update items and scores in the list of results
					if (matches.size() < k || sim > simThreshold) {

						// Find the place in the lists where to add
						// the new score (and the corresponding item)
						place = matchScores.size() - 1;
						while ((place >= 0) && (matchScores.get(place) < sim)) {
							--place;
						}
						place++;

						// Add new score and item to the lists
						if (place < k) {
							matchScores.insert(place, sim);
							matches.insert(place, candidate);

							// Expel superfluous item and score from the
							// list
							if (matchScores.size() > k) {
								matchScores.removeAt(k);
								matches.removeAt(k);
							}
						}

						// Adjust threshold
						if (matchScores.size() >= k) {

							simThreshold = matchScores.get(matchScores.size() - 1);

							ratio = simThreshold / (1 + simThreshold);

							// Recompute bounds for prefix filtering
							// based on the updated threshold
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
		}

		for (int no = 0; no < matches.size(); no++) {
			matchKeys.add(targetCollection.keys[matches.get(no)]);
			numMatches++;
		}
//		System.out.println("Collected " + numMatches + " results from " + candidates.size() + " candidates.");
//		count++;

		return numMatches;
	}

}