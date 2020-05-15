package eu.smartdatalake.simsearch.measure;

/**
 * Implements a similarity measure based on an exponential decay function.
 * @param <V>  Type variable to represent the values involved in the similarity calculations (usually, double values).
 */
public class DecayedSimilarity<V> implements ISimilarity<V> {
	
	// Default decay constant not configured by the user
	double lambda;  
	
	// The factor to be used for scaling the original distance values
	Scaling scale;  
	
	// Distance measure to be used in calculating of similarity scores
	IDistance<V> distance = null; 
	
	// Serial number of the task that executes the query with this similarity measure
	int taskId;
	
	/**
	 * Constructor
	 * @param distance   Parameterized distance measure to be used for calculating similarity scores
	 * @param decay   The decay constant to be applied in similarity calculations.
	 * @param taskId  The task that executes the query with this similarity measure.
	 */
	public DecayedSimilarity(IDistance<V> distance, double decay, int taskId) {
		
		this.distance = distance;
		this.lambda = decay;
		this.scale = new Scaling(0.0);
		this.taskId = taskId;              	
	}
	
	/**
	 * Returns a similarity score in [0..1] according to the exponential decay function.
	 */
	@Override
	public double calc(V v) {

		if (v != null)
			return scoring(this.distance.calc(v));

		return 0.0;    // Default score: No similarity
	}

	/**
	 * Returns a similarity score in [0..1] according to the exponential decay function.
	 */
	@Override
	public double calc(V v1, V v2) {
		
		if ((v1 != null) && (v2 != null))
			return scoring(this.distance.calc(v1, v2));
				
		return 0.0;  // Default score: No similarity
	}

	/**
	 * Applies the exponential decay function on a given distance value.
	 */
	@Override
	public double scoring(double distance) {
		
		// Special handling of irrelevant tokens in categorical search
		if ((Math.abs(distance - 1.0) < 0.000001) && (this.distance.getClass().getSimpleName().equals("CategoricalDistance")))
			return 0.0;	
		else    // Otherwise, use scaling and the exponential decay function
			return Math.exp(- scale.apply(distance) * lambda);
	}


	@Override
	public IDistance<V> getDistanceMeasure() {
		return this.distance;
	}

	@Override
	public boolean isScaleSet() {
/*
		if (this.scale.isSet())
			System.out.println("SCALE: " + this.scale.getScale() + " for " + this.distance.getClass().getSimpleName());
*/		
		return this.scale.isSet();
	}
	

	@Override
	public int getTaskId() {
		return this.taskId;
	}
	
}
