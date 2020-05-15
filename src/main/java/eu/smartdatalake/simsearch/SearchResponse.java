package eu.smartdatalake.simsearch;

import eu.smartdatalake.simsearch.ranking.ResultPair;
import eu.smartdatalake.simsearch.ranking.RankedResult;

/**
 * Provides the complete response to a multi-attribute search query, which encapsulates the search results and their similarity matrix.
 */
public class SearchResponse {

	private String notification;
	private Double[] weights;
	private RankedResult[] rankedResults;
	private ResultPair[] similarityMatrix;
	
	public void setRankedResults(RankedResult[] results) {
		this.rankedResults = results;
	}
	
	public void setSimilarityMatrix(ResultPair[] matrix) {
		this.similarityMatrix = matrix;
	}

	public RankedResult[] getRankedResults() {
		return rankedResults;
	}

	public ResultPair[] getSimilarityMatrix() {
		return similarityMatrix;
	}

	public String getNotification() {
		return notification;
	}

	public void setNotification(String notification) {
		this.notification = notification;
	}

	public Double[] getWeights() {
		return weights;
	}

	public void setWeights(Double[] weights) {
		this.weights = weights;
	}

}
