package eu.smartdatalake.simsearch.ranking;

/**
 * Interface to the various rank aggregation methods
 */
public interface IRankAggregator {
	
	/**
	 * Executes the rank aggregation process.
	 * @return  Array of the aggregated top-k results.
	 */
	public RankedResult[][] proc();
	
}
