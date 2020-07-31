package eu.smartdatalake.simsearch;

import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Auxiliary class that provides information about the data sources (each named with their queryable column) available for similarity search operations.
 */
public class AttributeInfo {

	public List<String> column;    		// The column(s) (one or multiple attributes) that provide the data is the identifier for this data source
	public String operation;			// The type of search operation (categorical_topk, numerical_topk, spatial_knn) supported by this data source
	
	/**
	 * Default constructor
	 */
	public AttributeInfo() {	
	}
	
	/**
	 * Constructor
	 * @param column
	 * @param operation
	 */
	public AttributeInfo(String column, String operation) {
		this.column = Arrays.asList(column);
		this.operation = operation;
	}

	// GETTER and SETTER methods
	public String getColumn() {
		if (column.size() == 1)
			return column.get(0);
		else
			return Arrays.toString(column.toArray(new String[0]));
	}
	@JsonProperty("column")
	public void setColumn(String column) {
		this.column = Arrays.asList(column);
	}
	@JsonProperty("columns")
	public void setColumns(List<String> columns) {
		this.column = columns;
	}
	public String getOperation() {
		return operation;
	}
	public void setOperation(String operation) {
		this.operation = operation;
	}
	
}
