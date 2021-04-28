package eu.smartdatalake.simsearch.engine.processor.ranking;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Wrapper of a priority queue to hold candidate entities per attribute in rank aggregation.
 */
public class RankedList {

	ConcurrentLinkedQueue<PartialResult> list;
	
	public RankedList() {	
		this.list = new ConcurrentLinkedQueue<PartialResult>();
	}

	public boolean add(PartialResult res) {
		return list.add(res);
	}
	
	public boolean addAll(RankedList topkResults) {
		return list.addAll((Collection<? extends PartialResult>) topkResults);	
	}
	
	public Iterator<PartialResult> iterator() {
		return list.iterator();
	}
	
	public boolean isEmpty() {
		return list.isEmpty();
	}
	
	public int size( ) {
		return list.size();
	}
	
	public PartialResult poll() {
		return list.poll();
	}
	
	public PartialResult peek() {
		return list.peek();
	}
	
}
