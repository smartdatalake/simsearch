package eu.smartdatalake.simsearch.request;

import io.swagger.annotations.ApiModelProperty;

/**
 * Specifies the output file characteristics for multi-attribute similarity search requests.
 */
public class SearchOutput {

	@ApiModelProperty(required = false, allowableValues = "json, csv, txt", value = "The output format for search results; specify JSON (default), CSV, or tabular text (also issued in standard output).")
	public String format;
	
	@ApiModelProperty(required = false, value = "Delimiter character between columns in the output CSV file")
	public String delimiter;
	
	@ApiModelProperty(required = false, value = "Quote character for enclosing string values in the output CSV file")
	public String quote;
	
	@ApiModelProperty(required = false, value = "Indicates whether a header with column names will be written to the output CSV file")
	public Boolean header;
	
	@ApiModelProperty(required = false, value = "Path to the output file")
	public String file;
	
	@ApiModelProperty(required = false, value = "Names of any extra attributes (in existing data sources, but not involved in similarity criteria) to include in the output")
	public String[] extra_columns;
	
}
