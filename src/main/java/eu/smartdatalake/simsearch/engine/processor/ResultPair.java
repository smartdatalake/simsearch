package eu.smartdatalake.simsearch.engine.processor;

import io.swagger.annotations.ApiModelProperty;

/**
 * Auxiliary class that retains the similarity scores between two search results (entity).
 */
public class ResultPair {

	@ApiModelProperty(required = true, value = "Identifier of the first entity in the pair")
	private String left; 	// DatasetIdentifier for the first entity
	
	@ApiModelProperty(required = true, value = "Identifier of the second entity in the pair")
	private String right;	// DatasetIdentifier for the second entity
	
	@ApiModelProperty(required = true, value = "Estimated similarity score for this pair of entities")
	private double score; 	// Estimated similarity score for this pair of entities
	
	/**
	 * Constructor
	 * @param left  DatasetIdentifier for the first item
	 * @param right  DatasetIdentifier for the second item
	 */
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
