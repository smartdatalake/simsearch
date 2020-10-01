package eu.smartdatalake.simsearch.request;

import io.swagger.annotations.ApiModelProperty;

/**
 * Defines an attribute data source in a MountRequest.
 */
public class MountSource {

	@ApiModelProperty(required = true, notes = "The (unique) name of the data source")
	public String name;

	@ApiModelProperty(required = true, allowableValues = "csv, jdbc, restapi", notes = "The type of the data source")
	public String type;
	
	@ApiModelProperty(required = false, notes = "Path to the directory containing the CSV file with the data")
	public String directory;

	@ApiModelProperty(required = false, notes = "Driver required for JDBC connections")
	public String driver;
	
	@ApiModelProperty(required = false, notes = "Username for accessing to a DBMS over a JDBC connection or to a REST API over an HTTP connection")
	public String username;
	
	@ApiModelProperty(required = false, notes = "Password for accessing to a DBMS over a JDBC connection or to a REST API over an HTTP connection")
	public String password;
	
	@ApiModelProperty(required = false, notes = "Encoding of the data when using a JDBC connection")
	public String encoding;
	
	@ApiModelProperty(required = false, notes = "URL for JDBC or REST API connections; URL for directories in remote HTTP/FTP servers containing CSV data sources")
	public String url;
	
	@ApiModelProperty(required = false, notes = "Specification of API KEY for connecting to another instance of SimSearch service")
	public String api_key;
	
}
