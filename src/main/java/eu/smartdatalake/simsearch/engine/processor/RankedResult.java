package eu.smartdatalake.simsearch.engine.processor;

import eu.smartdatalake.simsearch.engine.IResult;

/**
 * Auxiliary class that is used to collect the attribute values and their scores for a final result, along with its ranking and aggregate score.
 */
public class RankedResult implements IResult {

	private String id;
	private double score;
	private int rank;
	private boolean exact;  // Indicates whether the ranking assigned to this result is exact or not (i.e., based on approximate bounds)
	private String name;	// Only used in reporting names of entities
	
	ResultFacet[] attributes;

	/**
	 * Constructor
	 * @param cardinality  The number of attributes involved in the search; their values will be listed in the final results.
	 */
	public RankedResult(int cardinality) {
		setExact(true);   	// By default, consider that the ranking for this result is exact
		attributes = new ResultFacet[cardinality];
	}

	/* GETTER methods */
	@Override
	public String getId() {
		return id;
	}
	
	@Override
	public double getScore() {
		return score;
	}
	
	public int getRank() {
		return rank;
	}
	
	@Override
	public ResultFacet[] getAttributes() {
		return this.attributes;
	}
	
	/* SETTER methods */
	public void setId(String id) {
		this.id = id;
	}

	@Override
	public void setScore(double score) {
		this.score = score;
	}

	public void setRank(int rank) {
		this.rank = rank;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	@Override
	public boolean isExact() {
		return exact;
	}

	public void setExact(boolean exact) {
		this.exact = exact;
	}
	
	/**
	 * Flatten all attribute values and return a unified string with a user-specified delimiter between values.
	 * @param delimiter  The delimiter character to separate values.
	 * @return  A CSV string for reporting the contents of this search result.
	 */
	@Override
	public String toString(String delimiter) {
		
		StringBuilder builder = new StringBuilder();
		builder.append(this.rank + delimiter);
		builder.append(this.id + delimiter);

		for (ResultFacet col : this.attributes) {
			builder.append(col.getValue() + delimiter);
			builder.append(col.getScore() + delimiter);
		}

		builder.append(this.score);
	    
		return builder.toString();
	}
	
	/**
	 * Flatten all attribute values and return a unified string with a user-specified delimiter between values.
	 * @param delimiter  The delimiter character to separate values.
	 * @param quote  The quote character to be used for string values in case they contain the delimiter character.
	 * @return  A CSV string for reporting the contents of this search result.
	 */
	@Override
	public String toString(String delimiter, String quote) {
		
		StringBuilder builder = new StringBuilder();
		builder.append(this.rank + delimiter);
		builder.append(this.id + delimiter);

		for (ResultFacet col : this.attributes) {
			if (col.getValue().contains(delimiter))
				builder.append(quote + col.getValue() + quote + delimiter);
			else
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
		
		for (ResultFacet col : this.attributes) {
			builder.append(col.getName() + "Val" + delimiter);
			builder.append(col.getName() + "Score" + delimiter);
		}
		
		builder.append("score");
	    
		return builder.toString();
	}

}
