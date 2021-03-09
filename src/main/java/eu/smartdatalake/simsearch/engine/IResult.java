package eu.smartdatalake.simsearch.engine;

/**
 * Interface to the results obtained from similarity search.
 */
public interface IResult {

	String getColumnNames(String outColumnDelimiter);

	String toString(String outColumnDelimiter);

	String toString(String outColumnDelimiter, String outQuote);

	String getId();

	void setId(String string);

	double getScore();

	public void setScore(double score);
	
	public void setName(String name);
	
}
