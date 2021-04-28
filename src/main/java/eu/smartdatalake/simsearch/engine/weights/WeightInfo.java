package eu.smartdatalake.simsearch.engine.weights;

/**
 * Retains an attribute name and its assigned weight in a similarity search query.
 */
public class WeightInfo {

	private String attribute;
	private double value;
	
	/**
	 * Constructor
	 * @param attr  The name of the attribute.
	 * @param val  The weight assigned to this attribute in similarity search.
	 */
	public WeightInfo(String attr, double val) {
		
		this.attribute = attr;
		this.value = val;
	}

	/**
	 * Prints the name of the attribute and its assigned weight. 
	 */
	public String toString() {
		return this.attribute + " -(w)-> " + this.value;
	}
	
	/**
	 * Provides the attribute name where this weight applies to.
	 * @return  The attribute name.
	 */
	public String getAttribute() {
		return this.attribute;
	}
	
	/**
	 * Provides the weight value applied the attribute.
	 * @return  A double number in [0..1] representing the weight.
	 */
	public double getValue() {
		return this.value;
	}
	
}
