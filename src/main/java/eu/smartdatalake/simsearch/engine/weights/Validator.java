package eu.smartdatalake.simsearch.engine.weights;

/**
 * Validates the weight values specified for an attribute involved in a similarity search query.
 */
public class Validator {

	/**
	 * Validates that all weight values specified for the given attribute are in [0..1]
	 * @param attr   The attribute name concerning these weight(s).
	 * @param weights  An array of double numbers representing alternative weights.
	 * @return  True, if all weights are in [0..1]; otherwise, False.
	 */
	public boolean check(String attr, Double[] weights) {
		
		for (Double w : weights)
			if ((w < 0.0) || (w > 1.0)) {
				return false;
			}

		return true;	
	}
	
}
