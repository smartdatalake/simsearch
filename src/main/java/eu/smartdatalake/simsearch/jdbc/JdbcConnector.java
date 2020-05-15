package eu.smartdatalake.simsearch.jdbc;

import java.sql.ResultSet;

/**
 * Interface that defines all methods to be implemented by a JDBC Database connector.
 */
public interface JdbcConnector {

  /**
   * Provides the name of the DBMS (e.g., POSTGRESQL, ORACLE) to which this connection is established.
   * @return
   */
  public String getDbSystem();
  
  /**
   * Returns the result of the SQL query executed against the database.
   * @param sql  An SQL command for the SELECT query.
   * @return  A resultset with all results of the query.
   */
  public ResultSet executeQuery(String sql);

  /**
   * Query that fetches only the first result; Assuming that one value is only needed.
   * @param sql  An SQL command for the SELECT query.
   * @return  An object representing a value (e.g., SRID, name of the primary key attribute, etc.).
   */
  public Object findSingletonValue(String sql);
  
  /**
   * Closes the connection to the database.
   */
  public void closeConnection();

  /** 
   * Establishes a new connection taken from the pool.
   * @param connPool  The pool offering available JDBC connections to the database.
   */
  public void openConnection(JdbcConnectionPool connPool);

}
