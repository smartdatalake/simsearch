package eu.smartdatalake.simsearch.request;

import io.swagger.annotations.ApiModelProperty;

public class MountSpecs {

	@ApiModelProperty(required = true, notes = "The unique name of the data source (directory, JDBC connection, or REST API)")
	public String source;
	
	@ApiModelProperty(required = true, notes = "The dataset with the attribute values to search against; e.g., a CSV file or a table in a DBMS")
	public String dataset;
	
	@ApiModelProperty(required = true, allowableValues = "spatial_knn, categorical_topk, numerical_topk", notes = "The similarity search operation supported for this attribute")
	public String operation;
	
	@ApiModelProperty(required = true, notes = "The queryable attribute in the specified dataset; for search on a composite attribute (e.g., location with lon/lat coordinates), specify an array of attribute names (e.g., ['lon','lat'])")
	public Object search_column;
	
	@ApiModelProperty(required = false, notes = "The primary key attribute containing unique identifiers for entities in the specified dataset; required for CSV files")
	public String key_column;		
	
	@ApiModelProperty(required = false, notes = "Separator character between columns; applicable for input CSV file only")
	public String separator;
	
	@ApiModelProperty(required = false, notes = "Indicates whether a header with column names exists; applicable for input CSV file only")
	public Boolean header;
	
	@ApiModelProperty(required = false, notes = "Delimiter characters between tokens (keywords)")
	public String token_delimiter;
	
	@ApiModelProperty(required = false, notes = "Maximum number or lines to be consumed; applicable for input CSV file only; omit if all data will used")
	public Integer max_lines;
	
	@ApiModelProperty(required = false, allowableValues = "z, unity", notes = "Normalization method to be applied over mumerical values; omit if no normalization should be applied")
	public String normalized;
}
