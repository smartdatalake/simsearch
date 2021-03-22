package eu.smartdatalake.simsearch.request;

import io.swagger.annotations.ApiModelProperty;

/**
 * Specifications of a similarity search query against a particular attribute.
 */
public class SearchSpecs {

	@ApiModelProperty(required = true, value = "The attribute to search against; for search on composite attribute (e.g., location with lon/lat coordinates), specify an array of attribute names")
	public Object column;

	@ApiModelProperty(required = true, value = "The value to search for similar ones; in case of categorical search, specify an array of string values (e.g., keywords)")
	public Object value;

	@ApiModelProperty(required = true, value = "An array of double values to be used as weights in ranking of results")
	public Double[] weights;

	@ApiModelProperty(required = false, value = "A positive double value used as decay factor in ranking of results")
	public Double decay;

	@ApiModelProperty(required = false, value = "A positive double value used as scale factor to normalize distance values on this attribute amongst results")
	public Double scale;
	
	@ApiModelProperty(required = false, value = "A filter to be applied over in-situ queried data (e.g., in a DBMS or Elasticsearch) prior to similarity search. This filter should have a syntax according to the data source, e.g., the condition in a WHERE clause in SQL or a filter context in Elasticsearch, and is being applied as-is against the data.")
	public String filter;
}
