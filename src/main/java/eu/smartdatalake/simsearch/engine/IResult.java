package eu.smartdatalake.simsearch.engine;

import eu.smartdatalake.simsearch.engine.processor.ResultFacet;
import io.swagger.annotations.ApiModelProperty;

/**
 * Interface to the results obtained from similarity search.
 */
public interface IResult {
	
	@ApiModelProperty(required = true, value = "Identifier of the resulting entity in the dataset")
	String getId();
	
	@ApiModelProperty(required = true, value = "Ranking of this result amongst the top-k returned by the query")
	public int getRank();
	
	String getColumnNames(String outColumnDelimiter);

	@ApiModelProperty(required = true, value = "Attribute values and similarity scores on the individual attributes specified in the query")
	ResultFacet[] getAttributes();
	
	@ApiModelProperty(required = true, value = "Estimated similarity score of this result to the query")
	double getScore();
	
	@ApiModelProperty(required = true, value = "Indicates whether the ranking is exact or approximate (in case the query timed out before issuing all top-k results)")
	public boolean isExact();
	
	String toString(String outColumnDelimiter);

	String toString(String outColumnDelimiter, String outQuote);

	void setId(String string);

	public void setScore(double score);

}
