package eu.smartdatalake.simsearch;

import eu.smartdatalake.simsearch.ranking.ResultPair;
import eu.smartdatalake.simsearch.ranking.RankedResult;

/**
 * Provides the complete response to a multi-attribute similarity search query, which encapsulates the search results and their similarity matrix.
 */
public class SearchResponse extends Response {

	private Double[] weights;
	private RankedResult[] rankedResults;
	private ResultPair[] similarityMatrix;
	
	/**
	 * Sets the final ranked results for the specified search request.
	 * @param results  Array of the top-k ranked results.
	 */
	public void setRankedResults(RankedResult[] results) {
		this.rankedResults = results;
	}
	
	/**
	 * Sets the k x k matrix that measures similarity for all pairwise combinations of the top-k results.
	 * @param matrix  A k x k similarity matrix.
	 */
	public void setSimilarityMatrix(ResultPair[] matrix) {
		this.similarityMatrix = matrix;
	}

	/**
	 * Provides the final ranked results for the specified search request.
	 * @return  Array of the top-k ranked results.
	 */
	public RankedResult[] getRankedResults() {
		return rankedResults;
	}

	/**
	 * Provides the k x k matrix that measures similarity for all pairwise combinations of the top-k results.
	 * @return  A k x k similarity matrix.
	 */
	public ResultPair[] getSimilarityMatrix() {
		return similarityMatrix;
	}

	/**
	 * Sets the weights used to compute the respective ranked results, as specified by the user when the request was submitted.
	 * @param weights  An array of double values (one per attribute involved in the search request).
	 */
	public void setWeights(Double[] weights) {
		this.weights = weights;
	}
	
	/**
	 * Provides the weights used to compute the respective ranked results, as specified by the user when the request was submitted.
	 * @return  An array of double values (one per attribute involved in the search request).
	 */
	public Double[] getWeights() {
		return weights;
	}

}
