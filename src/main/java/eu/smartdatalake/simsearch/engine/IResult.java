package eu.smartdatalake.simsearch.engine;

import eu.smartdatalake.simsearch.engine.processor.ResultFacet;

/**
 * Interface to the results obtained from similarity search.
 */
public interface IResult {

	String getColumnNames(String outColumnDelimiter);

	String toString(String outColumnDelimiter);

	String toString(String outColumnDelimiter, String outQuote);

	String getId();

	ResultFacet[] getAttributes();
	
	void setId(String string);

	double getScore();

	public void setScore(double score);
	
	public void setName(String name);
	
	public boolean isExact();
}
