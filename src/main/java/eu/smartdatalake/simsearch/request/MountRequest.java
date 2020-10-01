package eu.smartdatalake.simsearch.request;

import eu.smartdatalake.simsearch.IRequest;
import io.swagger.annotations.ApiModelProperty;

/**
 * Specification of a Mount request that makes particular data source(s) available for designated similarity search operation(s).
 */
public class MountRequest implements IRequest {

	@ApiModelProperty(required = false, notes = "Path to a text file that logs all activity")
	public String log;
	
	@ApiModelProperty(required = true, notes = "Connection details for data sources to be mounted")
	public MountSource[] sources;
	
	@ApiModelProperty(required = true, notes = "Specifications for the queryable attribute(s) and the similarity operation supported by each one")
	public MountSpecs[] search;
	
}
