package eu.smartdatalake.simsearch.ranking;

import java.util.BitSet;

import eu.smartdatalake.simsearch.engine.PartialResult;

/**
 * Auxiliary class that collects results along with upper and lower bounds on their aggregated scores.
 */
public class AggregateResult extends PartialResult {
	
	private BitSet appearances; // Bitmap indicating the queues where this value
								// has been seen so far
	private double lb; // Lower bound
	private double ub; // Upper bound
	private int size; // Bitmap size
	
	/**
	 * Constructor
	 * @param id  PartialResult identifier (the key of the object).
	 * @param size   Bitmap size: the number of results to be aggregated.
	 * @param lb  Lower bound of similarity scores.
	 * @param ub  Upper bound of similarity scores.
	 */
	public AggregateResult(String id, int size, double lb, double ub) {
		super(id, -1.0);
		this.size = size;
		appearances = new BitSet(size);
		appearances.clear();
		this.lb = lb;
		this.ub = ub;
	}

	public double getLowerBound() {
		return lb;
	}

	public double getUpperBound() {
		return ub;
	}

	public void setLowerBound(double lb) {
		this.lb = lb;
	}

	public void setUpperBound(double ub) {
		this.ub = ub;
	}
	
	public void setAppearance(int pos) {
		this.appearances.set(pos);
	}

	/**
	 * Checks if all results are available for the aggregation. 
	 * @return  True, if all constituent results are received.
	 */
	public boolean checkAppearance() {
		return (this.appearances.cardinality() == this.size);
	}

	public BitSet getAppearance() {
		return this.appearances;
	}

	public String print() {
		return this.getId() + "@(" + this.lb + "," + this.ub + ") " + appearances.toString();
	}
	
}
