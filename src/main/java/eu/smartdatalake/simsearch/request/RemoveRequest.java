package eu.smartdatalake.simsearch.request;

import eu.smartdatalake.simsearch.AttributeInfo;
import eu.smartdatalake.simsearch.IRequest;
import io.swagger.annotations.ApiModelProperty;

/**
 * Specification of a request that removes attribute data source(s) from those available for similarity search queries.
 */
public class RemoveRequest implements IRequest {

	@ApiModelProperty(required = true, notes = "Array of attribute names and their supported similarity search operations to be removed.")
	public AttributeInfo[] remove;
}
