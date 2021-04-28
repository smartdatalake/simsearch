package eu.smartdatalake.simsearch.engine.measure;

/**
 * Auxiliary class that handles scaling of distance measures in order to be used in similarity scores.
 */
public class Scaling {

	private double scale;   // The value to be used for scaling the original distance values
	private boolean flag;	// Indicates whether a scale value has been set.
	
	public Scaling(double scale) {
		
		this.scale = scale;
		this.flag = (scale > 0.0) ? true : false;
	}

	/**
	 * Adjust the given distance value with the scaling factor.
	 * @param distance   Original distance value.
	 * @return  The rescaled distance value.
	 */
	public double apply(double distance) {
		
		// If scaling factor is not yet specified, set its value
		if (!flag) {
			if (distance > 0.0) {   // First result that differs from query value
				setScale(distance);		
//				System.out.println("Scale factor : " + this.scale);
			}
			else {  // Exact match
				return distance;	
			}
		}		
		
		return distance / this.scale;
	}
	
	/**
	 * Sets the scaling factor to be used in similarity scoring.
	 * @param scale  Numeric value representing the scale factor to be applied in all distance values.
	 */
	public void setScale(double scale) {
		this.scale = scale;
		this.flag = (scale > 0.0) ? true : false;
	}

	/**
	 * Provides the scale factor used.
	 * @return  A numeric value representing the scale factor.
	 */
	public double getScale() {
		return scale;
	}
	
	/**
	 * Checks whether the scale factor has been set.
	 * @return  True, if the scale factor is set; otherwise, False.
	 */
	public boolean isSet() {
		return flag;
	}
	
}
