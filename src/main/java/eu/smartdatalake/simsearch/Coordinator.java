package eu.smartdatalake.simsearch;

import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.smartdatalake.simsearch.csv.numerical.INormal;
import eu.smartdatalake.simsearch.jdbc.JdbcConnectionPool;
import eu.smartdatalake.simsearch.jdbc.JdbcConnector;
import eu.smartdatalake.simsearch.request.CatalogRequest;
import eu.smartdatalake.simsearch.request.MountRequest;
import eu.smartdatalake.simsearch.request.MountSource;
import eu.smartdatalake.simsearch.request.MountSpecs;
import eu.smartdatalake.simsearch.request.RemoveRequest;
import eu.smartdatalake.simsearch.request.SearchRequest;
import eu.smartdatalake.simsearch.restapi.HttpConnector;
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
            if (entry.getValue().getValueAttribute().equals(column)) {
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
	 * @param jsonFile  Path to the JSON configuration file of the data sources, attributes and operations to be used in subsequent search requests.
	 */
	public void mount(String jsonFile) {
		
		JSONObject config = parseConfig(jsonFile);		
		this.mount(config);
	}
	
	
	/**
	 * Mounting stage: Determining the data sources (CSV files or JDBC connections) to be used in searching.
	 * This method accepts a configuration formatted as a JSON object.
	 * @param mountConfig  JSON configuration file of the data sources, attributes and operations to be used in subsequent search requests.
	 */
	public void mount(JSONObject mountConfig) { 
		
		MountRequest params = null;
		ObjectMapper mapper = new ObjectMapper();	
		try {
			params = mapper.readValue(mountConfig.toJSONString(), MountRequest.class);
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		
		mount(params);
	}
	
	/**
	 * Mounting stage: Determining the data sources (CSV files or JDBC connections) to be used in searching.
	 * This method accepts a configuration formatted as a JSON object.
	 * @param params  Parameters specifying the data sources, attributes and operations to be used in subsequent search requests.
	 */
	public void mount(MountRequest params) {
		
		// log file
		try {
			if (log == null) {   // Use already existing log file if an index after query execution has started
				String logFile;
				if (params.log != null) 
					logFile = params.log;
				else   // Otherwise, use current system time to compose the name of the log file
					logFile = "SimSearch" + new SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date()) + ".log";
				log = new Logger(logFile, false);
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
					
		// Array of available data sources (directories or JDBC connections or REST APIs)
		MountSource[] sources = params.sources;
		
		// JDBC connections opened during mounting
		List<JdbcConnector> openJdbcConnections = new ArrayList<JdbcConnector>();
		
		if (sources != null) {
			// Iterate over the specified directories or JDBC connections or REST APIs
			for (MountSource sourceConfig: sources) {

	        	DataSource dataSource = null;
	        	
	        	if (!dataSources.isEmpty())	        	
	        		dataSource = findDataSource(sourceConfig.name);
       	
	        	if (dataSource == null) {
	        		
		        	if (sourceConfig.type.equals("csv")) {   // This is a CSV data source
		        		dataSource = new DataSource(sourceConfig.name, sourceConfig.directory);
		        		dataSources.put(dataSource.getKey(), dataSource);
//		        		System.out.println("PATH: " + dataSources.get(connection.getKey()).getPathDir());
		        	}
		        	else if (sourceConfig.type.equals("jdbc")) {	// This is a JDBC connection
	
						// Specifications for the JDBC connection						
						String url = sourceConfig.url;
						String driver = sourceConfig.driver;
						String username = sourceConfig.username;
						String password = sourceConfig.password;
						String encoding = sourceConfig.encoding;
						if (encoding != null)
					    	encoding = encoding.toLowerCase();     //Values like "UTF-8", "ISO-8859-1", "ISO-8859-7"
						
						Properties props = new Properties();
					    props.put("charSet", encoding);
					    props.put("user", username);
					    props.put("password", password);
					    
					    JdbcConnectionPool jdbcConnPool = new JdbcConnectionPool(driver, url, props, log);
					
						// Remember this connection pool; this may be used for multiple queries
						if (jdbcConnPool != null) {	
							dataSource = new DataSource(sourceConfig.name, jdbcConnPool);
							dataSources.put(dataSource.getKey(), dataSource);
						}
		        	}
		        	else if (sourceConfig.type.equals("restapi")) {   // This is a REST API data source, e.g., Elasticsearch	        		
		        		try {
		        			HttpConnector httpConn = null;
		        			// If authorization (API KEY) is specified by the user, then use it 'as is' in the header of requests
		        			if (sourceConfig.api_key != null)
		        				httpConn = new HttpConnector(new URI(sourceConfig.url), sourceConfig.api_key);
		        			else
		        				httpConn = new HttpConnector(new URI(sourceConfig.url));
			        			
							// Remember this connection; this may be used for successive queries
							if (httpConn != null) {	
								dataSource = new DataSource(sourceConfig.name, httpConn);
								
								// Include type specification for safe discrimination between REST API services
								if (sourceConfig.url.contains("simsearch"))
									dataSource.setSimSearchService(true);
								
								dataSources.put(dataSource.getKey(), dataSource);
							}
						} catch (URISyntaxException e) {
							e.printStackTrace();
						}
	        		
		        	}
	        	}
	        }
		}	

		// Array of available search operations	
		MountSpecs[] searchSpecs = params.search;
		
		// Create the dictionary of data sources (i.e., queryable attributes) available for search
		if (searchSpecs != null) {

			for (MountSpecs searchConfig: searchSpecs) {

				// operation
				String operation = searchConfig.operation;
				
				// Path to directory or name of JDBC connection
				String sourceId = searchConfig.source;
				
				if (!dataSources.containsKey(sourceId)) {
					log.writeln("Data source with name " + sourceId + " has not been specified and will be ignored during search.");
					continue;
				}
							
				// Column to search; its name is used to identify the search operation against its data
				int colQuery = Constants.SEARCH_COLUMN;
				String colValueName = searchConfig.search_column.toString();
				
				// Dataset: CSV file or DBMS table
				String dataset = searchConfig.dataset;

				JdbcConnector jdbcConn = null;
				if (dataSources.get(sourceId).getJdbcConnPool() != null) {   // Querying a DBMS
					jdbcConn = dataSources.get(sourceId).getJdbcConnPool().initDbConnector();   // Required for identifying columns during the mounting phase
					openJdbcConnections.add(jdbcConn); 
					colQuery = myAssistant.getColumnNumber(dataset, colValueName, jdbcConn);
					log.writeln("Established connection to " + dataSources.get(sourceId).getJdbcConnPool().getDbSystem() + " for data column " + colValueName);
				}
				else if (dataSources.get(sourceId).getHttpConn() != null) {   // Querying a REST API 
					log.writeln("Connections to REST API " + dataSources.get(sourceId).getHttpConn().toString() + " will be used for queries.");
				}
				else if (dataSources.get(sourceId).getPathDir() != null) {   // Otherwise, querying a CSV file
					// CAUTION! Directory and file name get concatenated
					File sourcePath = new File(dataSources.get(sourceId).getPathDir());
					File filePath = new File(sourcePath, dataset);
					dataset = filePath.toString();
					
					String columnDelimiter = Constants.COLUMN_SEPARATOR;
					if (searchConfig.separator!= null) {
						columnDelimiter = searchConfig.separator;
						if (columnDelimiter == null || columnDelimiter.equals(""))
							columnDelimiter = Constants.COLUMN_SEPARATOR;
					}
					
					// Attribute of the dataset involved in search; 
					// CAUTION! This may involve multiple columns currently supported only for lon, lat coordinates
					if (searchConfig.search_column != null) {
						colValueName = searchConfig.search_column.toString();
						// If header exists in the CSV file, identify the column
						if ((searchConfig.header != null) && (searchConfig.header == true)) {     
							colQuery = myAssistant.getColumnNumber(dataset, colValueName, columnDelimiter);
							if (colQuery < 0) {
								log.writeln("Attribute name " + colValueName + " is not found in the input data! No queries can target this attribute.");
								continue;
							}
						}
					}				
				}
				else {
					log.writeln("Incomplete specifications to allow search against data source " + sourceId);
					continue;
				}					

				// DatasetIdentifier to be used for all constructs built for this attribute
				DatasetIdentifier id = new DatasetIdentifier(dataSources.get(sourceId), dataset, colValueName);
			
				// Specify whether this dataset can be involved in similarity search queries
				if (searchConfig.queryable != null)
					id.setQueryable(searchConfig.queryable);
				
				// Specify an optional prefix to be used for entity identifiers in the results
				if (searchConfig.prefixURL != null)
					id.setPrefixURL(searchConfig.prefixURL);
				
				// If specified, also keep the name of the search attribute in the identifier
				id.setValueAttribute(colValueName);   
				
				// If specified, also keep the name of the key attribute in the identifier
				String colKeyName = null;
				if (searchConfig.key_column != null)
					colKeyName = searchConfig.key_column;
				id.setKeyAttribute(colKeyName);  				
				
				// Skip index construction if an index is already built on this attribute
				if (existsIdentifier(id)) {
					log.writeln("Attribute " + id.getValueAttribute() + " in dataset " + dataset +  " for " + operation + " has already been defined. Superfluous specification will be ignored.");
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
				else if (dataSources.get(sourceId).getHttpConn() != null) {    // REST API
					log.writeln("Data on " + colValueName + " from REST API is not ingested but will be queried directly.");
				}
				else if ((jdbcConn != null)    // Target is a DBMS table, but NOT indexed on this particular attribute
					&& !myAssistant.isJDBCColumnIndexed(dataset, colValueName, jdbcConn)) {	
//					System.out.println("Attempting to ingest data from " + dataset + " on column " + colValueName);
					// So, ingest its contents and create an in-memory index, exactly like the ones read from CSV files
					index(searchConfig, id, jdbcConn);
					// If no data source is available for data ingested from JDBC, create one
					if (dataSources.get("Ingested-data-from-JDBC") == null) {
						DataSource dataSource = new DataSource("Ingested-data-from-JDBC", "IN-MEMORY");  // Special values for this artificial data source
						dataSource.setJdbcConnPool(null);
						dataSource.setInSitu(false);
						dataSources.put(dataSource.getKey(), dataSource);
//						System.out.println("Ingested data source created with key:" + dataSource.getKey());
					}
					// In any subsequent search request, the dataset identifier should point to the in-memory data, NOT the database!
					id.setDataSource(dataSources.get("Ingested-data-from-JDBC"));
				}
				
				// Keep a reference to the attribute identifier
				datasetIdentifiers.put(id.getHashKey(), id);
	        }	        
		}
		
		// Close any database connections no longer required during the mounting stage
		for (JdbcConnector jdbcConn: openJdbcConnections)
			jdbcConn.closeConnection();
		
	}

	
	/**
	 * Indexing stage: Build all indices and keep in-memory arrays of (key,value) pairs for all indexed attributes.
	 * This method accepts a configuration formatted as a JSON object.
	 * @param attrConfig  Specifications for reading attribute values and creating indices to be used in subsequent search requests.
	 * @param id  Identifier of the dataset containing attribute values.
	 * @param jdbcConn  JDBC connection details for querying data in-situ from a DBMS.
	 */
	public void index(MountSpecs attrConfig, DatasetIdentifier id, JdbcConnector jdbcConn) {
				
		// Re-use existing index builder if additional indices need be constructed on-the-fly during query execution
		if (idxBuilder == null)
			idxBuilder = new IndexBuilder(log);
		
		// If data comes from a REST API, no indexing is required
		if (id.getDataSource().getHttpConn() != null)
			return;
		
		idxBuilder.proc(attrConfig, id, jdbcConn);
			
		datasets = idxBuilder.getDatasets();
		indices = idxBuilder.getIndices();
		normalizations = idxBuilder.getNormalizations();			
	}
	
	
	/**
	 * Discard all structures (indices, in memory look-ups) created on the given attribute(s) according to user-specified configurations.
	 * @param jsonFile   Path to the JSON configuration file of the attributes and operations to be removed.
	 */
	public void delete(String jsonFile) {
		
		JSONObject config = parseConfig(jsonFile);		
		this.delete(config);
	}
	

	/**
	 * Discard all structures (indices, in memory look-ups) created on the given attribute(s) according to user-specified configurations.
	 * @param removeConfig  JSON configuration for attributes and operations to be removed; these must have been previously specified in a mount request.
	 */
	public void delete(JSONObject removeConfig) {
		
		RemoveRequest params = null;
		ObjectMapper mapper = new ObjectMapper();	
		try {
			params = mapper.readValue(removeConfig.toJSONString(), RemoveRequest.class);
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		
		delete(params);
	}
	
	
	/**
	 * Discard all structures (indices, in memory look-ups) created on a given attribute according to user-specified parameters.
	 * @param params  Parameters specifying the attributes and operations to be removed; these must have been previously specified in a mount request.
	 */
	public void delete(RemoveRequest params) {
		
		// Array of specified data attributes and their supported operations to remove
		AttributeInfo[] arrAttrs2Remove = params.remove;
		
		if (arrAttrs2Remove != null) {
			for (AttributeInfo it: arrAttrs2Remove) {

				// operation
				String operation = it.getOperation();
				
				// attribute in an input dataset
				String colValueName = it.getColumn();					
				
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
					log.writeln("Removed support for attribute " + id.getValueAttribute() + " from dataset " + id.getDatasetName() + " for " + operation + ".");				
				}
				else
					continue;
	        }
		}
	}

	
	/**
	 * Catalog with the collection of all attributes available for querying; each dataset is named after a particular attribute.
	 * @return  An array of all attribute names including their supported similarity search operations.
	 */
	public AttributeInfo[] listDataSources() {
		
		AttributeInfo[] dataSources = new AttributeInfo[this.datasetIdentifiers.size()];
		int i = 0;
		for (DatasetIdentifier id: this.datasetIdentifiers.values()) {
			if (id.isQueryable()) {
				dataSources[i] = new AttributeInfo(id.getValueAttribute(), myAssistant.descOperation(id.getOperation()));
				i++;
			}
		}
		return Arrays.copyOf(dataSources, i);   // In case some entries have been skipped
	}

	/**
	 * Provides a catalog with the collection of attributes available for querying; if an operation is specified, only relevant attributes will be reported.
	 * This method accepts a file with the configuration settings.
	 * @param jsonFile   Path to the JSON configuration file that specifies an operation.
	 * @return An array of attribute names including their supported similarity search operation(s).
	 */
	public AttributeInfo[] listDataSources(String jsonFile) {
		
		JSONObject config = parseConfig(jsonFile);		
		return this.listDataSources(config);
	}

	/**
	 * Provides a catalog with the collection of attributes available for querying; if an operation is specified, only relevant attributes will be reported.
	 * This method accepts a configuration formatted as a JSON object.
	 * @param jsonFile   Path to the JSON configuration file that specifies an operation.
	 * @return An array of attribute names including their supported similarity search operation(s).
	 */
	public AttributeInfo[] listDataSources(JSONObject catalogConfig) {

		CatalogRequest params = null;
		
		ObjectMapper mapper = new ObjectMapper();	
		try {
			params = mapper.readValue(catalogConfig.toJSONString(), CatalogRequest.class);
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		
		return listDataSources(params);
	}
	
	
	/**
	 * Provides a catalog with the collection of attributes available for querying; if an operation is specified, only relevant attributes will be reported.
	 * This method accepts an instance of CatalogRequest class.
	 * @param catalogConfig  An instance of a CatalogRequest class; if not null, it should specify a string for similarity search operation (categorical_topk, spatial_knn, numerical_topk)
	 * @return  An array of attribute names along with the supported similarity search operation(s).
	 */
	public AttributeInfo[] listDataSources(CatalogRequest catalogConfig) {
		
		String operation = catalogConfig.operation;
		
		// If no specific operation is given, report all attributes available for search
		if (operation == null)
			return listDataSources();
		
		// Otherwise, report only those attributes supporting the specified operation
		List<AttributeInfo> dataSources = new ArrayList<AttributeInfo>();
		for (DatasetIdentifier id: this.datasetIdentifiers.values()) {
			if (operation.equalsIgnoreCase(myAssistant.descOperation(id.getOperation()))) {
				dataSources.add(new AttributeInfo(id.getValueAttribute(), myAssistant.descOperation(id.getOperation())));
			}
		}
		return dataSources.toArray(new AttributeInfo[dataSources.size()]);
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
	 * @param searchConfig   JSON configuration that provides the multi-facet query specifications.
	 * @return   A JSON-formatted response with the ranked results.
	 */
	public SearchResponse[] search(JSONObject searchConfig) {

		SearchRequest params = null;
		
		ObjectMapper mapper = new ObjectMapper();	
		try {
			params = mapper.readValue(searchConfig.toJSONString(), SearchRequest.class);
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		
		return search(params);
	}
	
	
	/**
	 * Searching stage: Given a user-specified configuration, execute the various similarity search queries and provide the ranked aggregated results.
	 * This method accepts an instance of SearchRequest class.
	 * @param params   An instance of SearchRequest class with the multi-facet search query specifications.
	 * @return   A JSON-formatted response with the ranked results.
	 */
	public SearchResponse[] search(SearchRequest params) {

		// A new handler is created for each request
		SearchHandler reqHandler = new SearchHandler(dataSources, datasetIdentifiers, datasets, indices, normalizations, log);
		
		return reqHandler.search(params);
	}
	
}
