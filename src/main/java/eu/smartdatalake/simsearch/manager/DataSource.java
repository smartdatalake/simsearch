package eu.smartdatalake.simsearch.manager;

import eu.smartdatalake.simsearch.jdbc.JdbcConnectionPool;
import eu.smartdatalake.simsearch.restapi.HttpConnector;

/**
 * Instantiates a connection to retrieve attribute values from one of the following possible data sources:
 * 		(i)   a path to a CSV file; or 
 * 		(ii)  a JDBC connection pool; or 
 * 		(iii) an HTTP connection to a REST API.
 */
public class DataSource {

	private String key;    						// A key identifier for this data source
	private String pathDir;						// Specifically for CSV data sources
	private JdbcConnectionPool jdbcConnPool;   	// Specifically for JDBC sources
	private HttpConnector httpConn;   			// Specifically for REST API sources
	private boolean inSitu;						// Distinguishes a data source queried in-situ (true) from an ingested one (false).
	private boolean isSimSearchService;			// Determines if this data source is a SimSearch REST API (true); otherwise, false.
	
	/**
	 * Constructor for JDBC data sources
	 * @param key  Internal identifier used for this data source.
	 * @param jdbcConnPool  JDBC connection pool for connecting to a DBMS.
	 */
	public DataSource(String key, JdbcConnectionPool jdbcConnPool) {
		this.key = key;
		this.jdbcConnPool = jdbcConnPool;
		this.inSitu = true;
		this.pathDir  = null;
		this.httpConn = null;
	}
	
	/**
	 * Constructor for CSV data sources
	 * @param key  Internal identifier used for this data source.
	 * @param pathDir  Directory on local disk or URL in a remote HTTP/FTP server that contains CSV data files.
	 */
	public DataSource(String key, String pathDir) {
		this.key = key;
		this.pathDir = pathDir;
		this.jdbcConnPool = null;
		this.httpConn = null;
		this.inSitu = false;
	}
	
	/**
	 * Constructor for REST API data sources
	 * @param key  Internal identifier used for this data source.
	 * @param httpConn  HTTP connection for accessing REST APIs (or another instance of a SimSearch service).
	 */
	public DataSource(String key, HttpConnector httpConn) {
		this.key = key;
		this.httpConn = httpConn;
		this.inSitu = true;
		this.pathDir = null;
		this.jdbcConnPool = null;
	}

	// SETTER methods
	
	public void setKey(String key) {
		this.key = key;
	}
	
	public void setPathDir(String pathDir) {
		this.pathDir = pathDir;
	}

	public void setJdbcConnPool(JdbcConnectionPool jdbcConnPool) {
		this.jdbcConnPool = jdbcConnPool;
	}

	public void setHttpConn(HttpConnector httpConn) {
		this.httpConn = httpConn;
	}

	public void setInSitu(boolean inSitu) {
		this.inSitu = inSitu;
	}

	public void setSimSearchService(boolean isSimSearchService) {
		this.isSimSearchService = isSimSearchService;
	}
	
	// GETTER methods
	
	public String getKey() {
		return key;
	}	
	
	public String getPathDir() {
		return pathDir;
	}

	public JdbcConnectionPool getJdbcConnPool() {
		return jdbcConnPool;
	}

	public HttpConnector getHttpConn() {
		return httpConn;
	}
	
	public boolean isInSitu() {
		return inSitu;
	}
	
	public boolean isSimSearchService() {
		return isSimSearchService;
	}
	
}
