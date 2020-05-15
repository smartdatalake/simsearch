package eu.smartdatalake.simsearch.ranking;

import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

/**
 * Auxiliary class to hold aggregate results at the current iteration of the rank aggregation process.
 */
public class AggregateResultCollection {
	
	private HashMap<String, AggregateResult> mapResults;

	public AggregateResultCollection() {
		mapResults = new HashMap<String, AggregateResult>();
	}
	
	public HashMap<String, AggregateResult> getMapResults() {
		return mapResults;
	}

	public void setMapResults(HashMap<String, AggregateResult> mapResults) {
		this.mapResults = mapResults;
	}
	
	public AggregateResult get(String key) {
		return mapResults.get(key);
	}
	
	public void put(String key, AggregateResult value) {
		mapResults.put(key, value);
	}

	public Set<String> keySet() {
		return mapResults.keySet();
	}
	
	public Collection<AggregateResult> values() {
		return mapResults.values();
	}
	
	public int size() {
		return mapResults.size();
	}
	
	public void remove(String key) {
		mapResults.remove(key);
	}
}
