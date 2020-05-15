package eu.smartdatalake.simsearch;

import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import eu.smartdatalake.simsearch.csv.numerical.INormal;
import eu.smartdatalake.simsearch.jdbc.JdbcConnectionPool;
import eu.smartdatalake.simsearch.jdbc.JdbcConnector;
import eu.smartdatalake.simsearch.csv.Index;
import eu.smartdatalake.simsearch.csv.IndexBuilder;

/**
 * Orchestrates indexing and multi-attribute similarity search and issues progressively ranked aggregated top-k results.
 * Available ranking methods: Threshold, No Random Access.
 * Provides two basic functionalities:	(1) Mounting: establishes data connections and prepares indices for the specified attributes;
 * 										(2) Search: Handles multi-facet similarity search requests.
 * Extra data connections (along with possible indexing) can be established even in case queries have been previously submitted against other attributes.
 */
public class Coordinator {

	Logger log;
	
	// Constructs required for mounting the available data sources
	Map<String, INormal> normalizations;
	Map<String, HashMap<?,?>> datasets;
	Map<String, DataSource> dataSources;
	Map<String, Index> indices;
	Map<String, DatasetIdentifier> datasetIdentifiers;
	
	IndexBuilder idxBuilder;
	
	Assistant myAssistant;
	
	/**
	 * Constructor
	 */
	public Coordinator() {
		
		normalizations = new HashMap<String, INormal>();
		datasets = new HashMap<String, HashMap<?,?>>();
		dataSources = new HashMap<String, DataSource>();
		indices = null;
		datasetIdentifiers = new HashMap<String, DatasetIdentifier>();
		idxBuilder = null;
		log = null;
		myAssistant = new Assistant();
	}
	

	/**
	 * Checks if a data source has already been specified on the requested facet (i.e., attribute in a dataset).
	 * @param q   The requested identifier.
	 * @return  True, if identifier exists; otherwise, False.
	 */
	private boolean existsIdentifier(DatasetIdentifier q) {
		
		for (Entry<String, DatasetIdentifier> entry : datasetIdentifiers.entrySet()) {
            if (entry.getValue().getHashKey().equals(q.getHashKey())) {
            	return true;
            }
        }
	
		return false;
	}


	/**
	 * Finds the internal identifier used for the data of a given attribute.
	 * @param column   The name of the attribute.
	 * @return  The dataset identifier.
	 */
	private DatasetIdentifier findIdentifier(String column) {
		
		for (Entry<String, DatasetIdentifier> entry : datasetIdentifiers.entrySet()) {
            if (entry.getValue().getColumnName().equals(column)) {
            	return entry.getValue();
            }
        }
	
		return null;
	}
	
	
	/**
	 * Parses a user-specified configuration for indexing or searching.
	 * @param configFile  Path to the configuration file in JSON format.
	 * @return  A JSON representation of the user-specified parameters.
	 */
	private JSONObject parseConfig(String configFile) {
		
		JSONParser jsonParser = new JSONParser();
		JSONObject config = null;
		try {
			config = (JSONObject) jsonParser.parse(new FileReader(configFile));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return config;
	}
	
	
	/**
	 * Identifies whether a data source (to a directory of a JDBC connection to a database) is already defined.
	 * @param key   A unique identifier of the data source, usually specified by the administrator.
	 * @return  An instance of a DataSource object representing the details of the connection.
	 */
	private DataSource findDataSource(String key) {
		
		for (Map.Entry<String, DataSource> conn: this.dataSources.entrySet()) {
			if ((conn != null) && (key.equals(conn.getKey())))
				return conn.getValue();			
		}
		
		return null;	
	}
	
	
	/**
	 * Mounting stage: Determining the data sources (CSV files or JDBC connections) to be used in searching.
	 * This method accepts a file with the configuration settings.
	 * @param jsonFile  Path to the JSON configuration file of the data sources, attributes and indices to be used in subsequent search requests.
	 */
	public void mount(String jsonFile) {
		
		JSONObject config = parseConfig(jsonFile);		
		this.mount(config);
	}
	
	
	/**
	 * Mounting stage: Determining the data sources (CSV files or JDBC connections) to be used in searching.
	 * This method accepts a configuration formatted as a JSON object.
	 * @param config  JSON configuration file of the data sources, attributes and indices to be used in subsequent search requests.
	 */
	public void mount(JSONObject config) {
		
		// log file
		try {
			if (log == null) {   // Use already existing log file if an index after query execution has started
				String logFile;
				if (config.get("log_file") != null) 
					logFile = String.valueOf(config.get("log_file"));
				else   // Otherwise, use current system time to compose the name of the log file
					logFile = "SimSearch" + new SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date()) + ".log";
				log = new Logger(logFile, false);
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
					
		// Array of available data connections (directories or JDBC connections)
		JSONArray sources = (JSONArray) config.get("sources");

		// JDBC connections opened during mounting
		List<JdbcConnector> openJdbcConnections = new ArrayList<JdbcConnector>();
		
		if (sources != null) {
			// Iterate over the specified directories or JDBC connections
	        Iterator it = sources.iterator();
	        while (it.hasNext()) {

	        	JSONObject sourceConfig = (JSONObject) it.next();
	        	DataSource dataSource = null;
	        	
	        	if (!dataSources.isEmpty())	        	
	        		dataSource = findDataSource(String.valueOf(sourceConfig.get("name")));
       	
	        	if (dataSource == null) {
	        		
		        	if (sourceConfig.get("type").equals("csv")) {   // This is a CSV data source
		        		dataSource = new DataSource(String.valueOf(sourceConfig.get("name")), String.valueOf(sourceConfig.get("directory")));
		        		dataSources.put(dataSource.getKey(), dataSource);
//		        		System.out.println("PATH: " + dataSources.get(connection.getKey()).getPathDir());
		        	}
		        	else if (sourceConfig.get("type").equals("jdbc")) {	// This is a JDBC connection
	
						// Specifications for the JDBC connection						
						String url = String.valueOf(sourceConfig.get("url"));
						String driver = String.valueOf(sourceConfig.get("driver"));
						String username = String.valueOf(sourceConfig.get("username"));
						String password = String.valueOf(sourceConfig.get("password"));
						String encoding = String.valueOf(sourceConfig.get("encoding"));
						if (encoding != null)
					    	encoding = encoding.toLowerCase();     //Values like "UTF-8", "ISO-8859-1", "ISO-8859-7"
						
						Properties props = new Properties();
					    props.put("charSet", encoding);
					    props.put("user", username);
					    props.put("password", password);
					    
					    JdbcConnectionPool jdbcConnPool = new JdbcConnectionPool(driver, url, props, log);
					
						// Remember this connection pool; this may be used for multiple queries
						if (jdbcConnPool != null) {	
							dataSource = new DataSource(String.valueOf(sourceConfig.get("name")), jdbcConnPool);
							dataSources.put(dataSource.getKey(), dataSource);
						}
		        	}
	        	}
	        }
		}
		

		// Array of available search operations
		JSONArray searches = (JSONArray) config.get("search");
			
		//TODO: Create here the dictionary of available data sources
		if (searches != null) {
	        Iterator it = searches.iterator();
	        while (it.hasNext()) {

	        	JSONObject searchConfig = (JSONObject) it.next();
	        	
				// operation
				String operation = String.valueOf(searchConfig.get("operation"));
				
				// Path to directory or name of JDBC connection
				String sourceId = String.valueOf(searchConfig.get("data_source"));
				
				if (!dataSources.containsKey(sourceId)) {
					log.writeln("Data source with name " + sourceId + " has not been specified and will be ignored during search.");
					continue;
				}
							
				int colQuery = Constants.SEARCH_COLUMN;
				String colValueName = null;
				
				// Dataset: CSV file or DBMS table
				String dataset = String.valueOf(searchConfig.get("dataset"));

				// Column name that will be used as identifier in search operations against this data
				colValueName = String.valueOf(searchConfig.get("search_column"));
				
				JdbcConnector jdbcConn = null;
				if (dataSources.get(sourceId).getJdbcConnPool() != null) {   // Querying a DBMS
					jdbcConn = dataSources.get(sourceId).getJdbcConnPool().initDbConnector();   // Required for identifying columns during the mounting phase
					openJdbcConnections.add(jdbcConn);
					colQuery = myAssistant.getColumnNumber(dataset, colValueName, jdbcConn);
					log.writeln("Established connection to " + dataSources.get(sourceId).getJdbcConnPool().getDbSystem() + " for data column " + colValueName);
				}
				else {   // Otherwise, querying a CSV file by default
					// CAUTION! Directory and file name get concatenated
					File sourcePath = new File(dataSources.get(sourceId).getPathDir());
					File filePath = new File(sourcePath, dataset);
					dataset = filePath.toString();
					
					String columnDelimiter = Constants.COLUMN_SEPARATOR;
					if (searchConfig.get("column_delimiter") != null) {
						columnDelimiter = String.valueOf(searchConfig.get("column_delimiter"));
						if (columnDelimiter == null || columnDelimiter.equals(""))
							columnDelimiter = Constants.COLUMN_SEPARATOR;
					}
					
					// Attribute of the dataset involved in search; 
					// CAUTION! This may involve multiple columns currently supported only for lon, lat coordinates
					if (searchConfig.get("search_column") != null) {
						colValueName = String.valueOf(searchConfig.get("search_column"));
						// If header exists in the CSV file, identify the column
						if (Boolean.parseBoolean(String.valueOf(searchConfig.get("header"))) == true) {     
							colQuery = myAssistant.getColumnNumber(dataset, colValueName, columnDelimiter);
							if (colQuery < 0) {
								log.writeln("Attribute name " + colValueName + " is not found in the input data! No queries can target this attribute.");
								continue;
							}
						}
					}				
				}

				//DatasetIdentifier to be used for all constructs built for this attribute
				DatasetIdentifier id = new DatasetIdentifier(dataSources.get(sourceId), dataset, colQuery);
			
				// If specified, also keep the name of the attribute in the identifier
				id.setColumnName(colValueName);   
				
				// Skip index construction if an index is already built on this attribute
				if (existsIdentifier(id)) {
					log.writeln("Attribute " + id.getColumnName() + " in dataset " + dataset +  " for " + operation + " has already been defined. Superfluous specification will be ignored.");
					continue;
				}
				
				// Retain type of operation for the identifier
				switch(operation.toLowerCase()) {
				case "categorical_topk":
					id.setOperation(Constants.CATEGORICAL_TOPK);
					break;
				case "numerical_topk":
					id.setOperation(Constants.NUMERICAL_TOPK);
					break;
				case "spatial_knn":
					id.setOperation(Constants.SPATIAL_KNN);
					break;
		        default:
		        	log.writeln("Unknown operation specified: " + operation);
		        	continue;
		        }
				
				// Target dataset is a CSV file, so it must be indexed according to the specifications
				if (dataSources.get(sourceId).getPathDir() != null) {
					index(searchConfig, id, null);
				}
				else if ((jdbcConn != null)    // Target is a DBMS table, but NOT indexed on this particular attribute
					&& !myAssistant.isJDBCColumnIndexed(dataset, colValueName, jdbcConn)) {
					// In that case, create an in-memory index from its contents, exactly like the ones read from CSV files
					// TODO: In this case, the dataset identifier should point to the in-memory data, NOT the database!
					index(searchConfig, id, jdbcConn);
				}
				
				// Keep a reference to the attribute identifier
				datasetIdentifiers.put(id.getHashKey(), id);
	        }	        
		}
		
		// Close any database connections no longer required for mounting
		for (JdbcConnector jdbcConn: openJdbcConnections)
			jdbcConn.closeConnection();
		
	}
	
	/**
	 * Indexing stage: Build all indices and keep in-memory arrays of (key,value) pairs for all indexed attributes.
	 * This method accepts a configuration formatted as a JSON object.
	 * @param config  JSON configuration of the attributes and indices to be used in subsequent search requests.
	 */
	public void index(JSONObject config, DatasetIdentifier id, JdbcConnector jdbcConn) {
				
		// Re-use existing index builder if additional indices need be constructed on-the-fly during query execution
		if (idxBuilder == null)
			idxBuilder = new IndexBuilder(log);
		idxBuilder.proc(config, id, jdbcConn);
			
		datasets = idxBuilder.getDatasets();
		indices = idxBuilder.getIndices();
		normalizations = idxBuilder.getNormalizations();			
	}
	
	
	/**
	 * Discards constructs (indices, in memory look-ups) previously created. 
	 * @param jsonFile   Path to the JSON configuration file of the attributes and indices previously built for search requests.
	 */
	public void delete(String jsonFile) {
		
		JSONObject config = parseConfig(jsonFile);		
		this.delete(config);
	}
	

	/**
	 * Discard all structures (indices, in memory look-ups) created on a given attribute according to user-specified configurations.
	 * @param config  JSON configuration for all indices to be constructed prior to query execution.
	 */
	public void delete(JSONObject config) {
				
		JSONArray arrRemove = null;
				
		// Array of specified data attributes and their supported operations to remove
		arrRemove = (JSONArray) config.get("remove");
			
		if (arrRemove != null) {
	        Iterator it = arrRemove.iterator();
	        while (it.hasNext()) {

	        	JSONObject removeConfig = (JSONObject) it.next();

				// operation
				String operation = String.valueOf(removeConfig.get("operation"));
				
				// input dataset
				String colValueName = String.valueOf(removeConfig.get("search_column"));					
				
				//DatasetIdentifier used for all constructs built for this attribute
				DatasetIdentifier id = findIdentifier(colValueName);				
		
				if (id == null) {
					log.writeln("No dataset with attribute " + colValueName + " is available for search.");
					throw new NullPointerException("No dataset with attribute " + colValueName + " is available for search.");
				}

				// If this operation is already supported on this attribute, then remove it
				if (existsIdentifier(id) && (operation.equals(myAssistant.descOperation(id.operation)))) {
					// Remove all references to constructs on this attribute data using its previously assigned hash key
					datasetIdentifiers.remove(id.getHashKey());
					datasets.remove(id.getHashKey());
					indices.remove(id.getHashKey());
					normalizations.remove(id.getHashKey());
					log.writeln("Removed support for attribute " + id.getColumnName() + " from dataset " + id.getDatasetName() + " for " + operation + ".");				
				}
				else
					continue;
	        }
		}
	}
	
	
	/**
	 * Provides the collection of datasets available for querying; each dataset is named after a particular attribute.
	 * @return  An array of dataset names including their supported similarity search operations.
	 */
	public DatasetInfo[] listDataSources() {
		
		DatasetInfo[] dataSources = new DatasetInfo[this.datasetIdentifiers.size()];
		int i = 0;
		for (DatasetIdentifier id: this.datasetIdentifiers.values()) {
			dataSources[i] = new DatasetInfo(id.getColumnName(), myAssistant.descOperation(id.getOperation()));
			i++;
		}
		return dataSources;
	}
	
	
	/**
	 * Searching stage: Given a user-specified configuration, execute the various similarity search queries and provide the ranked aggregated results.
	 * This method accepts a file with the configuration settings.
	 * @param jsonFile   Path to the JSON configuration file that provides the multi-facet query specifications.
	 * @return  A JSON-formatted response with the ranked results.
	 */
	public SearchResponse[] search(String jsonFile) {
		
		JSONObject config = parseConfig(jsonFile);
		
		return search(config);
	}
	
	
	/**
	 * Searching stage: Given a user-specified configuration, execute the various similarity search queries and provide the ranked aggregated results.
	 * This method accepts a configuration formatted as a JSON object.
	 * @param config   JSON configuration that provides the multi-facet query specifications.
	 * @return   A JSON-formatted response with the ranked results.
	 */
	public SearchResponse[] search(JSONObject config) {

		// A new handler is created for each request
		RequestHandler reqHandler = new RequestHandler(dataSources, datasetIdentifiers, datasets, indices, normalizations, log);
		
		return reqHandler.search(config);
	}
	
}
