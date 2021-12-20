package eu.smartdatalake.simsearch.engine.processor;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import eu.smartdatalake.simsearch.engine.IResult;
import io.swagger.annotations.ApiModelProperty;

/**
 * Auxiliary class that is used to collect the attribute values and their scores for a final result, along with its ranking and aggregate score.
 */
public class RankedResult implements IResult {

	@ApiModelProperty(required = true, value = "Identifier of the resulting entity in the dataset")
	private String id;
	
	@ApiModelProperty(required = true, value = "Estimated similarity score of this result to the query")
	private double score;
	
	@ApiModelProperty(required = true, value = "Ranking of this result amongst the top-k returned by the query")
	private int rank;
	
	@ApiModelProperty(required = true, value = "Indicates whether the ranking is exact or approximate (in case the query timed out before issuing all top-k results)")
	private boolean exact;  // Indicates whether the ranking assigned to this result is exact or not (i.e., based on approximate bounds)
	
	@ApiModelProperty(required = true, value = "Attribute values and similarity scores on the individual attributes specified in the query")
	ResultFacet[] attributes;

	@ApiModelProperty(required = false, value = "Attribute values regarding extra columns not involved in similarity search for reporting in output")
	private Map<String, ?> extra_attributes;
	
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
	
	@Override
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
		builder.append(this.score + delimiter);
		builder.append(this.id + delimiter);

		for (ResultFacet col : this.attributes) {
			Object val = col.getValue();
			if (val instanceof String[])
				builder.append(Arrays.toString((String[])val) + delimiter);
			else
				builder.append(val + delimiter);
			builder.append(col.getScore() + delimiter);
		}
		
		return StringUtils.chop(builder.toString());  // Expunge the delimiter char at the end
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
		builder.append(this.score + delimiter);
		builder.append(this.id + delimiter);

		for (ResultFacet col : this.attributes) {
			Object val = col.getValue();
			if (val instanceof String[])
				val = Arrays.toString((String[])val);
			if (val.toString().contains(delimiter))
				builder.append(quote + val + quote + delimiter);
			else
				builder.append(val + delimiter);
			builder.append(col.getScore() + delimiter);
		}

		return StringUtils.chop(builder.toString());  // Expunge the delimiter char at the end
	}
	
	/**
	 * Returns the column names to be used as header if results will be written in a CSV file.
	 * @param delimiter   The delimiter character to separate column names.
	 * @return  A CSV string for reporting the column names of the search results.
	 */
	public String getColumnNames(String delimiter) {
		
		StringBuilder builder = new StringBuilder();
		builder.append("rank" + delimiter);
		builder.append("score" + delimiter);
		builder.append("id" + delimiter);
		
		for (ResultFacet col : this.attributes) {
			builder.append(col.getName() + "Val" + delimiter);
			builder.append(col.getName() + "Score" + delimiter);
		}
		
		return StringUtils.chop(builder.toString());  // Expunge the delimiter char at the end
	}

	/** 
	 * Returns attribute values regarding extra columns not involved in similarity search.
	 * @return  A dictionary of values, one per extra attribute.
	 */
	public Map<String, ?> getExtraAttributes() {
		return extra_attributes;
	}

	/**
	 * Sets attribute values regarding extra columns not involved in similarity search.
	 * @param extra_attributes  A dictionary of values, one per extra attribute.
	 */
	public void setExtraAttributes(Map<String, ?> extra_attributes) {
		this.extra_attributes = extra_attributes;
	}

}
