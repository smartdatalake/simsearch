package eu.smartdatalake.simsearch.request;

import eu.smartdatalake.simsearch.engine.IRequest;
import io.swagger.annotations.ApiModelProperty;

/**
 * Specifications of a Catalog request to list the available attribute data sources and the similarity operation supported by each one.
 */
public class CatalogRequest implements IRequest {
	
	@ApiModelProperty(required = false, allowableValues = "spatial_knn, categorical_topk, numerical_topk, pivot_based", value = "Specify the similarity search operation and identify the attributes supporting it")
	public String operation;
	
	@ApiModelProperty(required = false, value = "Specify the attribute and identify if it is supported and for which operation(s)")
	public String column;
}
