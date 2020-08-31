package eu.smartdatalake.simsearch.request;

import eu.smartdatalake.simsearch.IRequest;
import io.swagger.annotations.ApiModelProperty;

public class CatalogRequest implements IRequest {
	
	@ApiModelProperty(required = false, allowableValues = "spatial_knn, categorical_topk, numerical_topk", notes = "Specify the similarity search operation and identify the attributes supporting it")
	public String operation;
	
}