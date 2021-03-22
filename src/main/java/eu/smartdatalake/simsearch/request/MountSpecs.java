package eu.smartdatalake.simsearch.request;

import io.swagger.annotations.ApiModelProperty;

/**
 * Specifications regarding the similarity search operation supported by an attribute data source in a MountRequest.
 */
public class MountSpecs {

	@ApiModelProperty(required = true, value = "The unique name of the data source (directory, JDBC connection, or REST API)")
	public String source;
	
	@ApiModelProperty(required = true, value = "The dataset with the attribute values to search against; e.g., a CSV file or a table in a DBMS")
	public String dataset;
	
	@ApiModelProperty(required = true, allowableValues = "spatial_knn, categorical_topk, numerical_topk, pivot_based, name_dictionary, keyword_dictionary", value = "The similarity search operation supported for this attribute or the dictionary to be constructed from its values")
	public String operation;

	@ApiModelProperty(required = false, allowableValues = "Manhattan, Euclidean, Chebyshev, Haversine, Jaccard", value = "The distance distance supported for this attribute (only applicable in PIVOT-based similarity search); if omitted, Euclidean is the default distance")
	public String metric;
	
	@ApiModelProperty(required = true, value = "The queryable attribute in the specified dataset; for search on a composite attribute (e.g., location with lon/lat coordinates), specify an array of attribute names (e.g., ['lon','lat'])")
	public Object search_column;
	
	@ApiModelProperty(required = false, value = "The primary key attribute containing unique identifiers for entities in the specified dataset; required for CSV files")
	public String key_column;		
	
	@ApiModelProperty(required = false, value = "Separator character between columns; applicable for input CSV file only")
	public String separator;
	
	@ApiModelProperty(required = false, value = "Indicates whether a header with column names exists; applicable for input CSV file only")
	public Boolean header;
	
	@ApiModelProperty(required = false, value = "Specifies a prefix to be combined with values from key_column and provide entity identifiers for the final results; if omitted, no URL identifiers will be created")
	public String prefixURL;
	
	@ApiModelProperty(required = false, value = "Indicates whether this attribute can be involved in similarity search queries; if omitted, it is set to true by default")
	public Boolean queryable;
	
	@ApiModelProperty(required = false, value = "Delimiter characters between tokens (keywords)")
	public String token_delimiter;
	
	@ApiModelProperty(required = false, value = "Maximum number or lines to be consumed; applicable for input CSV file only; omit if all data will used")
	public Integer max_lines;
	
	@ApiModelProperty(required = false, allowableValues = "z, unity", value = "Normalization method to be optionally applied over mumerical values; omit if no normalization should be applied")
	public String normalized;
	
	@ApiModelProperty(required = false, value = "Specifies the vocabulary (i.e., another attribute data source) that will be used to transform this data (e.g., from keywords to a numerical vector); omit if no transformation should be applied")
	public String transform_by;
	
	@ApiModelProperty(required = false, value = "Specifies the dataset created after transformation of this attribute data (e.g., a numerical vector obtained after transforming sets of keywords); omit if no transformation has been applied")
	public String transformed_to;
}
