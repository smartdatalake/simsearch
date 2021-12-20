package eu.smartdatalake.simsearch.request;

import eu.smartdatalake.simsearch.engine.IRequest;
import io.swagger.annotations.ApiModelProperty;

/**
 * Specifies parameters for multi-attribute similarity search requests.
 */
public class SearchRequest implements IRequest {

	@ApiModelProperty(required = true, value = "The number of top-k results to return")
	public int k;

	@ApiModelProperty(required = false, allowableValues = "threshold, partial_random_access, no_random_access, pivot_based", value = "The ranking method to apply in aggregation; if omitted, threshold algorithm is used by default")
	public String algorithm;
	
	@ApiModelProperty(required = false, value = "Specifications for writing search results to output file")
	public SearchOutput output;
	
	@ApiModelProperty(required = true, value = "The search conditions per attribute")
	public SearchSpecs[] queries;
	
}
