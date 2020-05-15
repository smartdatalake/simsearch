package eu.smartdatalake.simsearch.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * PostgreSQL/PostGIS implementation of JdbcConnector class.
 * FIXME: Check whether this is redundant, if every JDBC connection supports the same query functionality.
 */
public class PostgisDbConnector implements JdbcConnector {

  private Connection connection;
  private String dbSystem;
  
  
  /**
   * Constructor
   * @param conn
   * @param dbSystem
   */
  public PostgisDbConnector(Connection conn, String dbSystem) {
	  
	  this.connection = conn;
	  this.dbSystem = dbSystem;
  }
  

  /**
   * Returns the result of the SQL query executed against the PostgreSQL/PostGIS database.
   * @param sql  An SQL command for the SELECT query.
   * @return Resultset with all results of the query.
   */
  @Override
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

  
  @Override
  public Object findSingletonValue(String sql) {
  	
 	 Object val = null;
 	 try {
// 		 System.out.println(sql);
 		 Statement stmt = connection.createStatement();
 		 ResultSet rs = stmt.executeQuery(sql);
 		 if (rs.next())	
 			 val = rs.getObject(1);	 
// 		 System.out.println("Value: " + val);
 	 } catch (SQLException e) {
 		 e.printStackTrace();
 	 }
 	 
 	 return val;
  } 
  
   
  /**
   * Establishes a new connection to the PostgreSQL/PostGIS database.
   */
  @Override
  public void openConnection(JdbcConnectionPool connPool) {
   
	try {
		this.connection = connPool.getDataSource().getConnection();
	} catch (SQLException e) {
		System.out.println("Cannot connect to the JDBC pool.");
		e.printStackTrace();
	}
  }

  /**
   * Closes a connection to the PostgreSQL/PostGIS database.
   */
  @Override
  public void closeConnection() {
	  
    try {
      connection.close();
      connection = null;
    } catch (SQLException e) {
    	System.out.println("Cannot close connection to the database.");
    	e.printStackTrace();
    }
  }

	@Override
	public String getDbSystem() {
		
		return this.dbSystem;		
	}

}
