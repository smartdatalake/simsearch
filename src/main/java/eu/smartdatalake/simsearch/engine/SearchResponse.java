package eu.smartdatalake.simsearch.engine;

import eu.smartdatalake.simsearch.ranking.ResultPair;

/**
 * Provides the complete response to a multi-attribute similarity search query, which encapsulates the search results and their similarity matrix.
 */
public class SearchResponse extends Response {

	private Double[] weights;
	private IResult[] rankedResults;
	private ResultPair[] similarityMatrix;
	private double timeInSeconds;
	
	/**
	 * Sets the final ranked results for the specified search request.
	 * @param results  Array of the top-k ranked results.
	 */
	public void setRankedResults(IResult[] results) {
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
	public IResult[] getRankedResults() {
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

	/**
	 * Provides the execution cost of the search request.
	 * @return  The time required (in seconds) to execute this search request. 
	 */
	public double getTimeInSeconds() {
		return timeInSeconds;
	}

	/**
	 * Sets the execution cost (in seconds) of the search request.
	 * @param timeInSeconds  The time required (in seconds) to execute this search request. 
	 */
	public void setTimeInSeconds(double timeInSeconds) {
		this.timeInSeconds = timeInSeconds;
	}

}
