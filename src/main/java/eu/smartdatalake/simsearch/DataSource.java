package eu.smartdatalake.simsearch;


import eu.smartdatalake.simsearch.jdbc.JdbcConnectionPool;

/**
 * Instantiates a connection to a data source, either a path to a CSV file or a JDBC connection pool.
 */
public class DataSource {

	private String key;    				// A key identifier for this data source
	private String pathDir;				// Specifically for CSV data sources
	private JdbcConnectionPool jdbcConnPool;   // Specifically for JDBC sources
	
	/**
	 * Constructor for JDBC data sources
	 * @param key
	 * @param jdbcConnPool
	 */
	public DataSource(String key, JdbcConnectionPool jdbcConnPool) {
		this.key = key;
		this.jdbcConnPool = jdbcConnPool;
		this.pathDir  = null;
	}
	
	/**
	 * Constructor for CSV data sources
	 * @param key
	 * @param pathDir
	 */
	public DataSource(String key, String pathDir) {
		this.key = key;
		this.pathDir = pathDir;
		this.jdbcConnPool = null;
	}
	

	public void setKey(String key) {
		this.key = key;
	}
	
	public void setPathDir(String pathDir) {
		this.pathDir = pathDir;
	}
	public String getKey() {
		return key;
	}	
	
	public String getPathDir() {
		return pathDir;
	}

	public JdbcConnectionPool getJdbcConnPool() {
		return jdbcConnPool;
	}

	public void setJdbcConnPool(JdbcConnectionPool jdbcConnPool) {
		this.jdbcConnPool = jdbcConnPool;
	}
}
