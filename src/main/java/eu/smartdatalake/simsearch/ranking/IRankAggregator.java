package eu.smartdatalake.simsearch.ranking;

import eu.smartdatalake.simsearch.engine.IResult;

/**
 * Interface to the various rank aggregation methods
 */
public interface IRankAggregator {
	
	/**
	 * Executes the rank aggregation process.
	 * @return  Array of the aggregated top-k results.
	 */
	public IResult[][] proc();
	
}
