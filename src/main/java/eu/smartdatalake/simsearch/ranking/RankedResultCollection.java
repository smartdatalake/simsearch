package eu.smartdatalake.simsearch.ranking;

import java.util.ArrayList;
import java.util.List;

/**
 * Auxiliary class to compile ranked results.
 */
public class RankedResultCollection {
	
	private List<RankedResult> resultList;

	public RankedResultCollection() {
		this.resultList = new ArrayList<RankedResult>();
	}
	
	public List<RankedResult> getResultList() {
		return resultList;
	}

	public void setResultList(List<RankedResult> resultList) {
		this.resultList = resultList;
	}

	public int size() {
		return this.resultList.size();
	}

	public void add(RankedResult res) {
		resultList.add(res);
	}

	public RankedResult[] toArray() {
		return resultList.toArray(new RankedResult[resultList.size()]);		
	}	
}