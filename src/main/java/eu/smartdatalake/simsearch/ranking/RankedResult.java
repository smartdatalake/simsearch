package eu.smartdatalake.simsearch.ranking;

/**
 * Auxiliary class that is used to collect the attribute values and their scores for a final result, along with its ranking and aggregate score.
 */
public class RankedResult {

	private String id;
	private double score;
	private int rank;
	
	Attribute[] attributes;
	
	/**
	 * Constructor
	 */
	public RankedResult(int size) {
		attributes = new Attribute[size];
	}

	/* GETTER methods */
	public String getId() {
		return id;
	}
	
	public double getScore() {
		return score;
	}
	
	public int getRank() {
		return rank;
	}
	
	public Attribute[] getAttributes() {
		return this.attributes;
	}
	
	/* SETTER methods */
	public void setId(String id) {
		this.id = id;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public void setRank(int rank) {
		this.rank = rank;
	}
	
	/**
	 * Flatten all attribute values and return a unified string with a user-specified delimiter between values.
	 * @param delimiter  The delimiter character to separate values.
	 * @return  A CSV string for reporting the contents of this search result.
	 */
	public String toString(String delimiter) {
		
		StringBuilder builder = new StringBuilder();
		builder.append(this.rank + delimiter);
		builder.append(this.id + delimiter);

		for (Attribute col : this.attributes) {
			builder.append(col.getValue() + delimiter);
			builder.append(col.getScore() + delimiter);
		}

		builder.append(this.score);
	    
		return builder.toString();
	}
	
	/**
	 * Returns the column names to be used as header if results will be written in a CSV file.
	 * @param delimiter   The delimiter character to separate column names.
	 * @return  A CSV string for reporting the column names of the search results.
	 */
	public String getColumnNames(String delimiter) {
		
		StringBuilder builder = new StringBuilder();
		builder.append("rank" + delimiter);
		builder.append("id" + delimiter);
		
		for (Attribute col : this.attributes) {
			builder.append(col.getName() + "Val" + delimiter);
			builder.append(col.getName() + "Score" + delimiter);
		}
		
		builder.append("score");
	    
		return builder.toString();
	}
	
}
