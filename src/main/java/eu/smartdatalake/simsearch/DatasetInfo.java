package eu.smartdatalake.simsearch;

/**
 * Auxiliary class that provides information about the data sources (each named with their queryable column) available for similarity search operations.
 */
public class DatasetInfo {

	private String column;    			// The column (attribute) that provides the data is the identifier for this data source
	private String operation;			// The type of search operation (categorical_topk, numerical_topk, spatial_knn) supported by this data source
	
	/**
	 * Constructor
	 * @param column
	 * @param operation
	 */
	public DatasetInfo(String column, String operation) {
		this.column = column;
		this.operation = operation;
	}

	// GETTER and SETTER methods
	public String getColumn() {
		return column;
	}
	public void setColumn(String column) {
		this.column = column;
	}
	public String getOperation() {
		return operation;
	}
	public void setOperation(String operation) {
		this.operation = operation;
	}
	
}
