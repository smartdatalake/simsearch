package eu.smartdatalake.simsearch.manager;

/**
 * Defines the various data types for attributes supported in similarity search operations.
 */
public class DataType {

	private Type type;
	
	/**
	 * Enumeration of all possible data types; one must be specified per attribute.
	 */
    public enum Type {
    	UNKNOWN,
        STRING,
        NUMBER,
        GEOLOCATION,
        KEYWORD_SET,
        NUMBER_ARRAY;
    }

    /**
     * Constructor #1
     */
    public DataType() { 	
    }   

    /**
     * Constructor #2
     * @param type  The data type to set.
     */
    public DataType(Type type) {
    	this.type = type;
    }

    public boolean isUnknown() {
        return (getType() == Type.UNKNOWN) ? true : false;
    }
    
    public boolean isString() {
        return (getType() == Type.STRING) ? true : false;
    }

    public boolean isNumber() {
        return (getType() == Type.NUMBER) ? true : false;
    }
    
    public boolean isNumberArray() {
        return (getType() == Type.NUMBER_ARRAY) ? true : false;
    }
    
    public boolean isGeolocation() {
        return (getType() == Type.GEOLOCATION) ? true : false;
    }
    
    public boolean isKeywordSet() {
        return (getType() == Type.KEYWORD_SET) ? true : false;
    }
       
	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}
    
	public String toString() {
		return type.toString();
	}
	
}
