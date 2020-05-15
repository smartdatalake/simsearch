package eu.smartdatalake.simsearch.ranking;

/**
 * Auxiliary class that retains the similarity scores between two search results.
 */
public class ResultPair {

	private String left; 	// DatasetIdentifier for the first item
	private String right;	// DatasetIdentifier for the second item
	private double score; 	// Estimated similarity score for this pair of items
	
	public ResultPair(String left, String right) {
		this.left = left;
		this.right = right;
	}
	
	/* GETTER methods */
	public String getLeft() {
		return left;
	}
	
	public String getRight() {
		return right;
	}
	
	public double getScore() {
		return score;
	}
	
	/* SETTER methods */
	public void setLeft(String left) {
		this.left = left;
	}

	public void setRight(String right) {
		this.right = right;
	}

	public void setScore(double score) {
		this.score = score;
	}
	
}
