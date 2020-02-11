package eu.smartdatalake.simsearch;

/**
 * Auxiliary class that collects query results along with their scores.
 */
public class Result {
	private String id; // Result id
	private double score; // Assigned score

	/**
	 * Constructor
	 * @param id  Identifier of a result returned by a similarity search query.
	 * @param score  The similarity score of this result.
	 */
	public Result(String id, Double score) {
		this.id = id;
		this.score = score;
	}

	public String getId() {
		return id;
	}

	public double getScore() {
		return score;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setScore(double score) {
		this.score = score;
	}

	@Override
	public String toString() {
		return this.id + "@" + this.score;
	}
}