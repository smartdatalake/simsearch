package eu.smartdatalake.simsearch.engine.processor.ranking;

import java.util.ArrayList;
import java.util.List;

import eu.smartdatalake.simsearch.engine.IResult;

/**
 * Auxiliary class to compile ranked results.
 */
public class ResultCollection {
	
	private List<IResult> resultList;

	public ResultCollection() {
		this.resultList = new ArrayList<IResult>();
	}
	
	public List<IResult> getResultList() {
		return resultList;
	}

	public void setResultList(List<IResult> resultList) {
		this.resultList = resultList;
	}

	public int size() {
		return this.resultList.size();
	}

	public void add(IResult res) {
		resultList.add(res);
	}

	public IResult[] toArray() {
		return resultList.toArray(new IResult[resultList.size()]);		
	}	
	
	/**
	 * Checks whether the entity with the given identifier is included in the list.
	 * @param id  The identifier of an entity.
	 * @return  True, if the entity is in the list of compiled results; otherwise, False.
	 */
	public boolean contains(String id) {
		
		for (IResult res: resultList) {
			if (res.getId().equals(id))
				return true;
		}
		
		return false;
	}
}
