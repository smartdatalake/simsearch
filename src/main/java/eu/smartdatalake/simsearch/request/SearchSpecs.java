package eu.smartdatalake.simsearch.request;

import io.swagger.annotations.ApiModelProperty;

public class SearchSpecs {

	@ApiModelProperty(required = true, notes = "The attribute to search against; for search on composite attribute (e.g., location with lon/lat coordinates), specify an array of attribute names")
	public Object column;

	@ApiModelProperty(required = true, notes = "The value to search for similar ones; in case of categorical search, specify an array of string values (e.g., keywords)")
	public Object value;

	@ApiModelProperty(required = true, notes = "An array of double values to be used as weights in ranking of results")
	public Double[] weights;

	@ApiModelProperty(required = false, notes = "A positive double value used as decay factor in ranking of results")
	public Double decay;
}
