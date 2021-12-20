package eu.smartdatalake.simsearch.engine.processor;

import io.swagger.annotations.ApiModelProperty;

/**
 * Auxiliary class that is used to collect the (original) value and its score for a particular attribute (specified by a similarity search query) in the final result.
 */
public class ResultFacet {
		
	@ApiModelProperty(required = true, value = "The name of the attribute")
	private String name;    // Name of the attribute
	
	@ApiModelProperty(required = true, value = "Original value of the retrieved entity on this attribute")
	private Object value;   // Original value at this attribute in a record of the the dataset
	
	@ApiModelProperty(required = true, value = "Estimated similarity score of this attribute value to the respective query value")
	private double score;   // Estimated similarity score of this attribute value with the query value 
	
	/* GETTER methods */
	public String getName() {
		return name;
	}

	public Object getValue() {
		return value;
	}
	
	public double getScore() {
		return score;
	}
	
	/* SETTER methods */
	
	public void setName(String name) {
		this.name = name;
	}
	
	public void setValue(Object value) {
		this.value = value;
	}

	public void setScore(double score) {
		this.score = score;
	}
	
}
