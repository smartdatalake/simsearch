package eu.smartdatalake.simsearch.engine;

/**
 * Auxiliary class representing a result from searching against a single attribute along with the resulting similarity score.
 */
public class PartialResult<K,V> {
	private K id; 			// Result id
	private V value;		// Original attribute value
	private double score; 	// Assigned score

	/**
	 * Constructor #1
	 * @param id Object identifier of a single result returned by a similarity search query.
	 * @param val  The actual value in the searched attribute of this result.
	 */
	public PartialResult(K id, V val) {
		this.id = id;
		this.value = val;
	}
	
	/**
	 * Constructor #2
	 * @param id  Object identifier of a single result returned by a similarity search query.
	 * @param score  The similarity score of this result.
	 */
	public PartialResult(K id, Double score) {
		this.id = id;
		this.score = score;
	}

	/**
	 * Constructor #3
	 * @param id  Object identifier of a single result returned by a similarity search query.
	 * @param val  The actual value in the searched attribute of this result.
	 * @param score  The similarity score of this result.
	 */
	public PartialResult(K id, V val, Double score) {
		this.id = id;
		this.value = val;
		this.score = score;
	}
	
	
	@Override
	public String toString() {
		return this.id + "@" + this.score;
	}
	
	// GETTER methods
	
	public K getId() {
		return id;
	}

	public double getScore() {
		return score;
	}

	public V getValue() {
		return value;
	}
	
	// SETTER methods
	
	public void setId(K id) {
		this.id = id;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public void setValue(V value) {
		this.value = value;
	}

}
