package eu.smartdatalake.simsearch.request;

import eu.smartdatalake.simsearch.engine.IRequest;
import io.swagger.annotations.ApiModelProperty;

/**
 * Specification of a Mount request that makes particular data source(s) available for designated similarity search operation(s).
 */
public class MountRequest implements IRequest {

	@ApiModelProperty(required = false, value = "Path to a text file that logs all activity")
	public String log;

	@ApiModelProperty(required = false, value = "Total number of reference values that determines the dimensionality of the RR*-tree; applicable in pivot-based similarity search only")
	public Integer numPivots;
	
	@ApiModelProperty(required = true, value = "Connection details for data sources to be mounted")
	public MountSource[] sources;
	
	@ApiModelProperty(required = true, value = "Specifications for the queryable attribute(s) and the similarity operation supported by each one")
	public MountSpecs[] search;
	
}
