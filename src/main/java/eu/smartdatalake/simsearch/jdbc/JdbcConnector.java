package eu.smartdatalake.simsearch.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import eu.smartdatalake.simsearch.manager.IDataConnector;

/**
 * Class that defines all methods available by a JDBC Database connector.
 * Every JDBC connection to any DBMS should support the same query functionality.
 */
public class JdbcConnector implements IDataConnector {

  private Connection connection;
  private String dbSystem;
  
  
  /**
   * Constructor
   * @param conn  A connection (session) with a specific database.
   * @param dbSystem  The name of the DBMS (e.g., POSTGRESQL) as extracted from the respective JDBC driver.
   */
  public JdbcConnector(Connection conn, String dbSystem) {
	  
	  this.connection = conn;
	  this.dbSystem = dbSystem;
  }
  

  /**
   * Returns the result of the SQL query executed against the database.
   * @param sql  An SQL command for the SELECT query.
   * @return  A resultset with all results of the query.
   */
  public ResultSet executeQuery(String sql) {
	  
    ResultSet resultSet = null;
    try {
    	Statement stmt = connection.createStatement();
    	resultSet = stmt.executeQuery(sql);
    } catch (SQLException e) {
    	System.out.println("SQL query for data retrieval cannot be executed.");
    	e.printStackTrace();
    }
    return resultSet;
  }

  /**
   * Query that fetches only the first result; Assuming that one value is only needed.
   * @param sql  An SQL command for the SELECT query.
   * @return  An object representing a value (e.g., SRID, name of the primary key attribute, etc.).
   */
  @Override
  public Object findSingletonValue(String sql) {
  	
 	 Object val = null;
 	 try {
// 		 System.out.println(sql);
 		 Statement stmt = connection.createStatement();
 		 ResultSet rs = stmt.executeQuery(sql);
 		 if (rs.next())	
 			 val = rs.getObject(1);	 
 		 
 		 // FIXME: Special handling for NULL values in Avatica JDBC connections (Proteus)
 		 if ((dbSystem.equals("AVATICA")) && (val.equals("None")))
 			 val = null;
 		 
 	 } catch (SQLException e) {
 		 e.printStackTrace();
 	 }
 	 
 	 return val;
  } 
  
 
  /**
   * Assuming that primary key is a single attribute (column) for the given table name, this method can retrieve it.
   * @param tableName  The name of the table in the DBMS.
   * @return  The name of the primary key column.
   */
  public String getPrimaryKeyColumn(String tableName) {
	  
	// FIXME: Currently working for PostgreSQL only
	if (this.dbSystem.equals("POSTGRESQL")) {
		String sqlPrimaryKey = "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = '" + tableName + "'::regclass AND i.indisprimary;";
		return (String) findSingletonValue(sqlPrimaryKey);
	}
	else   // TODO: Handle other JDBC sources
		return null;
  }
  
  /** 
   * Establishes a new connection taken from the pool.
   * @param connPool  The pool offering available JDBC connections to the database.
   */
  public void openConnection(JdbcConnectionPool connPool) {
   
	try {
		this.connection = connPool.getDataSource().getConnection();
	} catch (SQLException e) {
		System.out.println("Cannot connect to the JDBC pool.");
		e.printStackTrace();
	}
  }

  /**
   * Closes a connection to the database.
   */
  public void closeConnection() {
	  
    try {
      connection.close();
      connection = null;
    } catch (SQLException e) {
    	System.out.println("Cannot close connection to the database.");
    	e.printStackTrace();
    }
  }

  /**
   * Provides the name of the DBMS to which this connection is established.
   * @return  The name of the DBMS (e.g., POSTGRESQL) as extracted from the respective JDBC driver.
   */
  public String getDbSystem() {

	  return this.dbSystem;
  }

}
