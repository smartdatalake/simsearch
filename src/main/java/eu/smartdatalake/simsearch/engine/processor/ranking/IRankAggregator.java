package eu.smartdatalake.simsearch.engine.processor.ranking;

import eu.smartdatalake.simsearch.engine.IResult;

/**
 * Interface to the various rank aggregation methods
 */
public interface IRankAggregator {
	
	/**
	 * Executes the rank aggregation process.
	 * @param query_timeout  Max execution time (in milliseconds) for ranking in a submitted query.
	 * @return  Array of the aggregated top-k results.
	 */
	public IResult[][] proc(long query_timeout);

}
