package eu.smartdatalake.simsearch.request;

import io.swagger.annotations.ApiModelProperty;

public class MountSource {

	@ApiModelProperty(required = true, notes = "The (unique) name of the data source")
	public String name;

	@ApiModelProperty(required = true, allowableValues = "csv, jdbc, restapi", notes = "The type of the data source")
	public String type;
	
	@ApiModelProperty(required = false, notes = "Path to the directory containing the CSV file with the data")
	public String directory;

	@ApiModelProperty(required = false, notes = "Driver required for JDBC connections")
	public String driver;
	
	@ApiModelProperty(required = false, notes = "Username for accessing to a DBMS over a JDBC connection")
	public String username;
	
	@ApiModelProperty(required = false, notes = "Password for accessing to a DBMS over a JDBC connection")
	public String password;
	
	@ApiModelProperty(required = false, notes = "Encoding of the data when using a JDBC connection")
	public String encoding;
	
	@ApiModelProperty(required = false, notes = "URL for JDBC or REST API connections")
	public String url;
	
	@ApiModelProperty(required = false, notes = "Specification of API KEY for connecting to another instance of SimSearch service")
	public String api_key;
	
}
