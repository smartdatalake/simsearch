package eu.smartdatalake.simsearch.pivoting;

import eu.smartdatalake.simsearch.engine.IDistance;
import eu.smartdatalake.simsearch.measure.DecayedSimilarity;
import eu.smartdatalake.simsearch.measure.Scaling;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Point;

/**
 * Calculates the similarity score for a given distance based on an exponential decay function.
 * This class inherits its functionality from the DecayedSimilarity used in rank aggregation.
 * @param <V>  Type variable to represent the values involved in the similarity calculations (usually, points with double ordinates).
 */
public class MetricSimilarity<V> extends DecayedSimilarity<V>{
	
	// Multi-dimensional query point with double ordinates; this point represents an attribute value (a location, a number, a vector representation of keywords, etc.)
	V baseElement;
	
	// The factor to be used for scaling the original distance values
	Scaling scale;  
	
	/**
	 * Constructor
	 * @param q  Multi-dimensional query point with (NOT embedded) double ordinates.
	 * @param distance  Metric distance to be used in calculating similarity scores.
	 * @param decay   The decay constant to be applied in similarity calculations.
	 * @param scale  The scale factor to normalize the computed distances.
	 * @param attrId  The serial number of the attribute in the internal representation of multi-dimensional embeddings.
	 */
	public MetricSimilarity(V q, IDistance distance, double decay, double scale, int attrId) {
		
		// Instantiate a DecayedSimilarity object to use in calculations
		super(distance, decay, scale, attrId);
		
		// Specify the query values (a multi-dimensional point for the specified attribute)
		this.baseElement = q;
		// As well as the scaling factors to be used in normalizing the distances values before exponential decay is applied
		this.scale = new Scaling(scale);
		this.scale.setScale(scale);			
	}
	
	
	/**
	 * Returns a similarity score in [0..1] of the given value (point) from the respective query value.
	 */
	public double calc(V v) {

		// Special check for NaN ordinates
		if ((v instanceof Point) && ((Point) v).containsNaN())
			return 0.0;  // Zero similarity

		// Otherwise, apply exponential decay function
		return super.calc(baseElement, v);
	}
	
}
