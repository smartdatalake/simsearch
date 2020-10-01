package eu.smartdatalake.simsearch.ranking;

/**
 * Auxiliary class that is used to collect the (original) value and its score for a particular attribute (specified by a similarity search query) in the final result.
 */
public class Attribute {
		
	private String name;    // Name of the attribute
	private String value;   // Original value at this attribute in a record of the the dataset
	private double score;   // Estimated similarity score of this attribute value with the query value 
	
	/* GETTER methods */
	public String getName() {
		return name;
	}

	public String getValue() {
		return value;
	}
	
	public double getScore() {
		return score;
	}
	
	/* SETTER methods */
	
	public void setName(String name) {
		this.name = name;
	}
	
	public void setValue(String value) {
		this.value = value;
	}

	public void setScore(double score) {
		this.score = score;
	}
	
}
