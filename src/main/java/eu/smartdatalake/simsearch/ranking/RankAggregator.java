package eu.smartdatalake.simsearch.ranking;
 
/**
 * Interface to the various rank aggregation methods
 */
public interface RankAggregator {
	
	/**
	 * Executes the rank aggregation process.
	 * @return  True, if the process is still running and less than top-k results have been issued; otherwise, False.
	 */
	public boolean proc();
}
