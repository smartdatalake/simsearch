package eu.smartdatalake.simsearch.manager;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import eu.smartdatalake.simsearch.manager.DataType.Type;

import com.fasterxml.jackson.annotation.JsonInclude.Include;

/**
 * Auxiliary class that provides information about the attribute data sources (each named with their queryable column) available for similarity search operations.
 */
@JsonInclude(Include.NON_NULL)    // Ignore NULL values when issuing the response
public class AttributeInfo {

	private List<String> column;    	// The column(s) (one or multiple attributes) that provide the data is the identifier for this data source
	private String operation;			// The type of search operation (categorical_topk, numerical_topk, spatial_knn, pivot_based) supported by this data source
	private Optional<Type> datatype = Optional.empty();
	private String sample;
	
	/**
	 * Constructor #1 (default)
	 */
	public AttributeInfo() {	
	}
	
	/**
	 * Constructor #2
	 * @param column  The attribute name(s) containing the data.
	 * @param operation  The similarity search operation supported on this attribute data.
	 */
	public AttributeInfo(String column, String operation) {
		this.column = Arrays.asList(column);
		this.operation = operation;
	}

	/**
	 * Constructor #3
	 * @param column  The attribute name(s) containing the data.
	 * @param operation  The similarity search operation supported on this attribute data.
	 * @param datatype  The data type of this queryable attribute.
	 */
	public AttributeInfo(String column, String operation, Type datatype) {
		this.column = Arrays.asList(column);
		this.operation = operation;
		this.datatype = Optional.ofNullable(datatype);
	}
	
	/**
	 * Constructor #4
	 * @param column  The attribute name(s) containing the data.
	 * @param operation  The similarity search operation supported on this attribute data.
	 * @param datatype  The data type of this queryable attribute.
	 * @param sample  A sample value in this attribute.
	 */
	public AttributeInfo(String column, String operation, Type datatype, String sample) {
		this.column = Arrays.asList(column);
		this.operation = operation;
		this.datatype = Optional.ofNullable(datatype);
		this.sample = sample;
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
	
	/**
	 * Provides the data type of the attribute values.
	 * @return  One of the queryable data types.
	 */
	public Type getDatatype() {
		// Return the data type, if specified
		if (datatype.isPresent())
            return datatype.get();
		
		return null;
	}

	/**
	 * Specifies the data type of this (queryable) attribute. 
	 * @param datatype  One of queryable data types.
	 */
	public void setDatatype(Type datatype) {
		this.datatype = Optional.ofNullable(datatype);
	}

	/**
	 * Provides an indicative value for this attribute.
	 * @return  A randomly chosen value extracted from the attribute data.
	 */
	public String getSampleValue() {
		return sample;
	}

	/**
	 * Retains a sample value as indicative for this dataset.
	 * @param sample  A randomly chosen value extracted from the attribute data.
	 */
	public void setSampleValue(String sample) {
		this.sample = sample;
	}
	
}
