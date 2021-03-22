package eu.smartdatalake.simsearch.request;

import io.swagger.annotations.ApiModelProperty;

/**
 * Specifies the output file characteristics for multi-attribute similarity search requests.
 */
public class SearchOutput {

	@ApiModelProperty(required = false, allowableValues = "json, csv", value = "The output format for search results; specify either JSON (default) or CSV.")
	public String format;
	
	@ApiModelProperty(required = false, value = "Delimiter character between columns in the output CSV file")
	public String delimiter;
	
	@ApiModelProperty(required = false, value = "Quote character for enclosing string values in the output CSV file")
	public String quote;
	
	@ApiModelProperty(required = false, value = "Indicates whether a header with column names will be written to the output CSV file")
	public Boolean header;
	
	@ApiModelProperty(required = false, value = "Path to the output file")
	public String file;
	
}
