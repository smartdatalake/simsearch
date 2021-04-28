package eu.smartdatalake.simsearch.engine.processor.ranking;

import java.util.ArrayList;
import java.util.List;

/**
 * Auxiliary class that is retains the already checked objects (i.e., their datasetIdentifiers) in order to avoid duplicate checks and thus suppress duplicates in the final results. 
 */
public class CheckedItems {
		
	private List<String> checkedKeys;

	public CheckedItems() {
		this.checkedKeys = new ArrayList<String>();
	}
	
	public List<String> getCheckedKeys() {
		return checkedKeys;
	}

	public void setCheckedKeys(List<String> checkedKeys) {
		this.checkedKeys = checkedKeys;
	}
	
	public void add(String key) {
		this.checkedKeys.add(key);
	}

	public boolean contains(String val) {
		return checkedKeys.contains(val);
	}
	
}
