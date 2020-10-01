package eu.smartdatalake.simsearch.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.postgis.PGgeometry;
import org.postgresql.util.PGobject;

import eu.smartdatalake.simsearch.Assistant;
import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.ISimSearch;
import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.PartialResult;
import eu.smartdatalake.simsearch.csv.categorical.TokenSet;
import eu.smartdatalake.simsearch.measure.ISimilarity;

/**
 * Implements functionality for in-situ queries of various types of similarity search (numerical, categorical, spatial) against a DBMS.
 * @param <K>  Type variable representing the keys of the stored objects (i.e., primary keys).
 * @param <V>  Type variable representing the values of the stored objects (i.e., their values on a given attribute).
 */
public class SimSearchQuery<K extends Comparable<? super K>, V> implements ISimSearch<K, V>, Runnable {

	Logger log = null;
	Assistant myAssistant;
	int operation;       		//Type of the search query
	String hashKey = null;   	// The unique hash key assigned to this search query
	
	JdbcConnector databaseConnector = null;   	// Instantiation of Connector class to a DBMS
	String dbType = null;
	
	Map<String, HashMap<K,V>> datasets = null;
	
	public int collectionSize;
	ConcurrentLinkedQueue<PartialResult> resultsQueue;

	String sql = null;			// SQL SELECT command to be composed for top-k search
	String viewClause ="";
	String distanceClause = "";
	String fromClause = "";
	String whereClause = "";
	String orderClause = "";
	String keyColumnName = null;
	String udfClause = "";
	
	// Compose the SQL SELECT command for value retrieval 
	public String sqlValueRetrievalTemplate = null;	

	ISimilarity simMeasure;
	
	public AtomicBoolean running = new AtomicBoolean(false);

	
	/**
	 * Constructor
	 * @param databaseConnector  The JDBC connection that provides access to the table.
	 * @param operation  The type of the similarity search query (0: CATEGORICAL_TOPK, 1: SPATIAL_KNN, 2: NUMERICAL_TOPK).
	 * @param tableName  The table name containing the attribute data used in the search.
	 * @param keyColumnName  Name of the attribute holding the entity identifiers (keys).
	 * @param valColumnName  Name of the attribute containing numerical values of these entities.
	 * @param searchValue  String specifying the query value according to the type os the search operation (i.e., keywords, a location, or a number).
	 * @param collectionSize  The count of results to fetch.
	 * @param simMeasure   The similarity measure to be used in the search.
	 * @param resultsQueue  Queue to collect query results.
	 * @param datasets  Dictionary of the attribute data available for search.
	 * @param hashKey  The unique hash key assigned to this search query.
	 * @param log  Handle to the log file for keeping messages and statistics.
	 */
	public SimSearchQuery(JdbcConnector databaseConnector, int operation, String tableName, String keyColumnName, String valColumnName, String searchValue, int collectionSize, ISimilarity simMeasure, ConcurrentLinkedQueue<PartialResult> resultsQueue, Map<String, HashMap<K,V>> datasets, String hashKey, Logger log) {
	  
		super();
		try
		{
    	  this.log = log;
    	  myAssistant = new Assistant();
    	  this.databaseConnector = databaseConnector;
    	  this.dbType = databaseConnector.getDbSystem();
    	  this.operation = operation;
    	  this.keyColumnName = keyColumnName;
    	  this.collectionSize = collectionSize;
    	  this.resultsQueue = resultsQueue;
    	  this.datasets = datasets;
    	  this.hashKey = hashKey;
    	  this.simMeasure = simMeasure;
    	  
    	  // In case no column for key datasetIdentifiers has been specified, use the primary key of the table
    	  if (this.keyColumnName == null) { 
    		  // Assuming that primary key is a single attribute (column) 
    		  // FIXME: SQL query to retrieve primary key attribute works for PostgreSQL only
    		  String sqlPrimaryKey = "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = '" + tableName + "'::regclass AND i.indisprimary;";	 
    		  this.keyColumnName = (String) this.databaseConnector.findSingletonValue(sqlPrimaryKey);
    	  }
    	  
    	  // Construct SQL clauses according to the type of the operation
    	  // FIXME: DBMSs may have different specifications for the various types of queries; currently using the PostgreSQL dialect
    	  switch(operation) {
	        case Constants.NUMERICAL_TOPK:
	        	udfClause = "abs(x: double): double := if (x < 0) then -x else x ";   // Specific UDF for use with Avatica JDBC (RAW + Proteus)
	        	distanceClause = valColumnName + ", abs(" + valColumnName + " - " + searchValue + ") AS distance";
	        	fromClause = tableName;
	        	orderClause = "distance";
	        	break;
	        case Constants.CATEGORICAL_TOPK:	
	        	// Rearrange array of keywords according to PostgreSQL requirements (i.e., separated by comma, enclosed in single quotes)
	        	searchValue = Arrays.stream(searchValue.split(";")).map(s -> String.format("'%s'", s)).collect(Collectors.joining(","));
	        	//searchValue = searchValue.replace('"', '\'');
	        	viewClause = "WITH token_arrays AS " + 
	        			"(SELECT " + this.keyColumnName + ", " + valColumnName + ", array_agg(elem) AS tokens " + 
	        			"FROM " + tableName + ", jsonb_array_elements_text(" + valColumnName + ") AS elem " + 
	        			"WHERE " + valColumnName + " ?| array["+ searchValue + "] " +
	        			"GROUP BY " + this.keyColumnName + ", " + valColumnName + ") ";
	        	distanceClause =  valColumnName + ", (1.0 - jaccard_similarity(tokens, array["+ searchValue +"])) AS distance ";
	        	fromClause = "token_arrays";
	        	orderClause = "distance";
	        	break;
	        case Constants.SPATIAL_KNN:
	        	// Only needed for PostGIS: Identify the SRID of the geometry column
	        	Object srid = this.databaseConnector.findSingletonValue("SELECT Find_SRID('', '" + tableName + "', '" + valColumnName + "')");
	        	distanceClause = valColumnName + ", ST_Distance(" + valColumnName + ", 'SRID=" + srid + ";" + searchValue + "'::geometry)";
	        	fromClause = tableName;
	        	orderClause = valColumnName + " <-> 'SRID=" + srid + ";" + searchValue + "'::geometry";
	        	break;
	        default:
	        	throw new IllegalArgumentException(Constants.INCORRECT_DBMS);
    	  }
    	  
    	  // Condition for excluding NULL values
    	  // FIXME: This is required for Avatica JDBC (Proteus)
    	  whereClause = valColumnName + " IS NOT NULL";
    	  
    	  // Template of SQL query to retrieve the value for a particular object ($id is a placeholder for its identifier)
    	  sqlValueRetrievalTemplate = "SELECT " + valColumnName + " FROM " + tableName + " WHERE " + this.keyColumnName + " = '$id'";
  	  
		} catch (Exception e) {
    	  this.log.writeln(Constants.INCORRECT_DBMS);      //Execution terminated abnormally
		  e.printStackTrace();
		}	
	}


	/**
	 * Connects to a database and retrieves records qualifying to the SQL SELECT query executed in-situ.
	 * @param k  The count of results to fetch.
	 * @param partialResults  The queue that collects results obtained from the specified query.
	 * @return  The number of collected results.
	 */
     public int compute(int k, ConcurrentLinkedQueue<PartialResult> partialResults) {
	
    	 int numMatches = 0;
    	 Object val = null;
    	 WKTReader wktReader = new WKTReader();
    	 ResultSet rs = null;
    	 long duration = System.nanoTime();
      
//    	 System.out.println(sql);
		 try {
			 //Execute SQL query in the DBMS and fetch all results 
			 rs = databaseConnector.executeQuery(sql.replace("$k$",""+k));
/*			 
			  // NOT USED: Identify the names of all columns
			  List<String> columns = new ArrayList<String>(rs.getMetaData().getColumnCount());
			  for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
				  columns.add(rs.getMetaData().getColumnName(i));
*/			  
			 // Iterate through all retrieved results and push them to the queue
			 // ASSUMPTION: three items per result: (1) the identifier; (2) attribute value; (3) distance
		     while (rs.next()) {  

		    	  // LOOK-UP STEP: Look-up the attribute value to be used during random access
		    	  val = rs.getObject(2);
		    	  try {	
		    		  if (val instanceof PGgeometry)
		    			  val = wktReader.read(val.toString().substring(val.toString().indexOf(";") + 1));
		    		  else if (val instanceof PGobject) {
		    			  // FIXME: Specific for PostgreSQL: Expunge [ and ] from the returned array, as well as double quotes
		    			  String keywords = val.toString().replace("\"", "");
		    			  keywords = keywords.substring(1, keywords.length()-1);
		    			  val = tokenize(rs.getString(1), keywords, ",");   // FIXME: comma is the delimiter in PostgreSQL arrays
		    		  }
		    	  } catch (ParseException e) {
		    			  e.printStackTrace();
		    	  }
		    	  // Casting the attribute value to the respective data type used by the look-up (hash) table
		    	  this.datasets.get(this.hashKey).put((K)rs.getObject(1), (V)val);
		    	  
		    	  // SCORING STEP: Result should get a score according to exponential decay function
		    	  if (this.dbType.equals("AVATICA")) {
		    		  // FIXME: Parsing double from strings is required by Avatica JDBC (Proteus)
			    	  partialResults.add(new PartialResult(rs.getString(1), val, simMeasure.scoring(Double.parseDouble(rs.getString(3)))));
		    	  }
		    	  else {
		    		  // FIXME: Parsing double from strings as required by PostgreSQL
		    		  partialResults.add(new PartialResult(rs.getString(1), val, simMeasure.scoring(rs.getDouble(3))));	
		    	  }
		    	  
		          numMatches++;  
		      }
		  }
		  catch(Exception e) { 
//				this.log.writeln("An error occurred while retrieving data from the database.");
				e.printStackTrace();
		  }
		 finally {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		 }
		 duration = System.nanoTime() - duration;
		 this.log.writeln("Query [" + myAssistant.descOperation(this.operation) + "] " + this.hashKey + " (in-situ) returned " + numMatches + " results in " + duration / 1000000000.0 + " sec.");
		 
		 return numMatches;                      //Report how many records have been retrieved from the database    
     }
     
     
 	/**
 	 * Progressively provides the next query result.
 	 * NOT applicable with this type of search, as results are issued directly to the queue.
 	 */
	@Override
	public List<V> getNextResult() {

		return null;
	}

	
	// FIXME: This method is also specified in the CategoricalValueFinder class.	
	private TokenSet tokenize(String id, String keywords, String tokDelimiter) {
		
		TokenSet set = null;
		try {
			if (keywords != null) {
				set = new TokenSet();
				set.id = id; 
				set.tokens = new ArrayList<String>();
				// Split and trim keywords
				List<String> tokens = new ArrayList<String>(new HashSet<String>(Arrays.asList(keywords.split("\\s*" + tokDelimiter + "\\s*"))));
				set.tokens.addAll(tokens);
//				tokens.forEach(System.out::println);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return set;
	}

	
	/**
	 * Executes the specified similarity search query in-situ against the database.
	 */
	public void run() {
		
	      //Determine constraint for top-k queries according to different SQL dialects
	      switch(this.dbType) {
	        case "POSTGRESQL":
	        	sql = viewClause + "SELECT " + this.keyColumnName + ", " + distanceClause + " FROM " + fromClause + " ORDER BY " + orderClause + " LIMIT $k$";
	          	break;
	        case "AVATICA":    // Connection to Proteus 
	        	sql = udfClause + "SELECT " + this.keyColumnName + ", " + distanceClause + " FROM " + fromClause + " WHERE " + whereClause + " ORDER BY " + orderClause + " LIMIT $k$";
	        	break;
	        default:
	        	this.log.writeln(Constants.INCORRECT_DBMS);
	        	throw new IllegalArgumentException(Constants.INCORRECT_DBMS);
	      }
	      
	      running.set(true);
	      
	      // Run the SQL query and populate the queue with its results
	      compute(this.collectionSize, this.resultsQueue);
	      
	      running.set(false);
	}
	
}
