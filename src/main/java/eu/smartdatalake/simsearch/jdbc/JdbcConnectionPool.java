package eu.smartdatalake.simsearch.jdbc;

import org.apache.commons.dbcp2.PoolableConnectionFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.Logger;

/**
 * Instantiates a connection pool to a database using JDBC.
 */
public class JdbcConnectionPool {

	Logger log = null;
	
	private GenericObjectPool<PoolableConnection> connPool = null;
	private PoolingDataSource<PoolableConnection> dataSource = null;
	private String dbSystem = null;
	
	/**
	 * Constructor
	 * @param driver  JDBC driver to be used for the connection.
	 * @param url  A string specifying the database connection URL.
	 * @param props  Properties to be used for the connection, including username and password.
	 * @param log   Handle to the log file for keeping messages and statistics.
	 */
	public JdbcConnectionPool(String driver, String url, Properties props, Logger log) {
		
		this.log = log;
		
		try {
	    	Class.forName(driver);
	    	
	    	this.setDbSystem(url.substring(url.indexOf("jdbc:")+5,url.indexOf(":", url.indexOf("jdbc:")+5)).toUpperCase());
			
		    // A ConnectionFactory that the pool will use to create connections; properties include at least username and password
		    DriverManagerConnectionFactory connectionFactory = new DriverManagerConnectionFactory(url, props);
		    		
		    // Implement the pooling functionality
		    PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
		    
		    // Pool configuration
		    GenericObjectPoolConfig<PoolableConnection> config = new GenericObjectPoolConfig<PoolableConnection>();
		    config.setMaxWaitMillis(500);
		    config.setMaxTotal(20);
		    config.setMaxIdle(5);
		    config.setMinIdle(5);
		    
		    // Create the pool...
		    connPool = new GenericObjectPool<PoolableConnection>(poolableConnectionFactory, config);
		    poolableConnectionFactory.setPool(connPool);
		    
		    // ... and a data source
		    dataSource = new PoolingDataSource<PoolableConnection>(connPool);
		    
		    this.log.writeln("JDBC connection pool created for " + url + ". " + checkPoolStatus());
		    
		} catch (Exception e) {
			e.printStackTrace();
		}    
	}


	/**
	 * Provides a JDBC DataSource making a pool of connections available.
	 * @return  A JDBC data source.
	 */
	public PoolingDataSource<PoolableConnection> getDataSource() {

		return dataSource;
    }


	/**
	 * Provides a pool of connections available for a JDBC data source.
	 * @return  A pool of connections.
	 */
    public GenericObjectPool<PoolableConnection> getConnectionPool() {
        return connPool;
    }
 
    /**
     * Notify about the status of this connection pool.
     * @return A string with statistics about connectivity to the JDBC source.
     */
    public String checkPoolStatus() {
    	
        return ("Connections: max: " + getConnectionPool().getMaxTotal() + "; active: " + getConnectionPool().getNumActive() + "; idle: " + getConnectionPool().getNumIdle());
    }

    /**
     * Provides a new JDBC connection for querying the underlying DBMS.
     * @return A JDBC connection that can be used for querying a specific attribute.
     */
    public JdbcConnector initDbConnector() {
    	
    	// Get a new connection from the pool
    	Connection conn = null;
  	  	try {
  			conn = dataSource.getConnection();
  		} catch (SQLException e) {
  			System.out.println("Cannot connect to the JDBC pool.");
  			e.printStackTrace();
  		}
  	  
		//Instantiation of Connector class to a DBMS
		JdbcConnector jdbcConnector = null;   
		try
		{
			//Determine connection type to the specified DBMS
			switch(dbSystem) {
				case "POSTGRESQL":  	// Connection to PostgreSQL/PostGIS
					jdbcConnector = new JdbcConnector(conn, dbSystem);
					break;
				case "AVATICA":   		// Connection to Proteus 
					jdbcConnector = new JdbcConnector(conn, dbSystem);
					break;
		        default:
		        	throw new IllegalArgumentException(Constants.INCORRECT_DBMS);
		        }
		} catch (Exception e) {
			System.out.println(Constants.INCORRECT_DBMS);      //Execution terminated abnormally
			e.printStackTrace();
		}
		
		return jdbcConnector;
    }

    
    /**
     * Provides the name of the DBMS that this connection pool has access to.
     * @return  The name of the DBMS (e.g., POSTGRESQL) as mentioned in the respective JDBC driver.
     */
	public String getDbSystem() {
		
		return dbSystem;
	}
	
	
	/**
	 * Specifies the name of the DBMS that this connection pool has access to.
	 * @param dbSystem  The name of the DBMS (e.g., POSTGRESQL) as mentioned in the respective JDBC driver.
	 */
	public void setDbSystem(String dbSystem) {
		
		this.dbSystem = dbSystem;
	}
    
}
