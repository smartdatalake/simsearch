package eu.smartdatalake.simsearch.request;

import eu.smartdatalake.simsearch.IRequest;
import io.swagger.annotations.ApiModelProperty;

public class SearchRequest implements IRequest {

	@ApiModelProperty(required = true, notes = "The number of top-k results to return")
	public int k;

	@ApiModelProperty(required = false, allowableValues = "threshold, partial_random_access, no_random_access", notes = "The ranking method to apply in aggregation; if omitted, threshold is used by default")
	public String algorithm;
	
	@ApiModelProperty(required = false, notes = "Specifications for writing search results to output file")
	public SearchOutput output;
	
	@ApiModelProperty(required = true, notes = "The search conditions per attribute")
	public SearchSpecs[] queries;
	
}
