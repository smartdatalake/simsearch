package eu.smartdatalake.simsearch;

import java.io.File;
import java.io.FileReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.smartdatalake.simsearch.engine.QueryValueParser;
import eu.smartdatalake.simsearch.engine.Response;
import eu.smartdatalake.simsearch.engine.SearchHandler;
import eu.smartdatalake.simsearch.engine.SearchResponse;
import eu.smartdatalake.simsearch.manager.AttributeInfo;
import eu.smartdatalake.simsearch.manager.DataSource;
import eu.smartdatalake.simsearch.manager.DataType;
import eu.smartdatalake.simsearch.manager.DataType.Type;
import eu.smartdatalake.simsearch.manager.ingested.DataIngestor;
import eu.smartdatalake.simsearch.manager.ingested.Index;
import eu.smartdatalake.simsearch.manager.ingested.lookup.Word2VectorTransformer;
import eu.smartdatalake.simsearch.manager.ingested.numerical.INormal;
import eu.smartdatalake.simsearch.manager.insitu.HttpRestConnector;
import eu.smartdatalake.simsearch.manager.insitu.JdbcConnectionPool;
import eu.smartdatalake.simsearch.manager.insitu.JdbcConnector;
import eu.smartdatalake.simsearch.manager.DatasetIdentifier;
import eu.smartdatalake.simsearch.manager.TransformedDatasetIdentifier;
import eu.smartdatalake.simsearch.pivoting.MetricReferences;
import eu.smartdatalake.simsearch.pivoting.PivotManager;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Point;
import eu.smartdatalake.simsearch.request.CatalogRequest;
import eu.smartdatalake.simsearch.request.MountRequest;
import eu.smartdatalake.simsearch.request.MountSource;
import eu.smartdatalake.simsearch.request.MountSpecs;
import eu.smartdatalake.simsearch.request.RemoveRequest;
import eu.smartdatalake.simsearch.request.SearchRequest;

/**
 * Orchestrates multi-attribute similarity search and issues ranked top-k results.
 * Available search algorithms: Threshold, No Random Access, Partial Random Access, Pivot-based.
 * Provides four basic functionalities:	(1) Mount: establishes data connections and prepares indices for the specified attribute data;
 * 										(2) Catalog: lists all attributes available for similarity search queries;
 * 										(3) Delete: removes attribute data source(s) from those available for similarity search;
 * 										(4) Search: handles multi-attribute similarity search requests.
 * Extra data connections (along with possible indexing) can be established even in case queries have been previously submitted against other attributes.
 */
public class Coordinator {

	Logger log;
	InstanceSettings instanceSettings;
	
	// Constructs required for mounting the available data sources
	Map<String, INormal> normalizations;
	Map<String, Map<?,?>> datasets;
	Map<String, DataSource> dataSources;
	Map<String, Index> indices;
	Map<String, DatasetIdentifier> datasetIdentifiers;
	
	DataIngestor dataIngestor;
	
	Assistant myAssistant;
	
	PivotManager pivotManager;
	
	private boolean collectQueryStats;
	
	/**
	 * Constructor
	 */
	public Coordinator() {
		
		normalizations = new HashMap<String, INormal>();
		datasets = new HashMap<String, Map<?,?>>();
		dataSources = new HashMap<String, DataSource>();
		indices = new HashMap<String, Index>();
		datasetIdentifiers = new HashMap<String, DatasetIdentifier>();
		dataIngestor = null;
		log = null;
		myAssistant = new Assistant();
		pivotManager = null;
		
		// By default, not collecting detailed statistics per query in normal execution
		this.collectQueryStats = false;
	}
	

	/**
	 * Checks if a data source has already been specified on the requested facet (i.e., attribute in a dataset).
	 * @param q  The requested identifier.
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
	 * Finds the internal identifier used for the data of a given attribute and its supported operation.
	 * @param column  The name of the attribute.
	 * @param operation  The name of the operation supported for this attribute.
	 * @return  The internal dataset identifier.
	 */
	private DatasetIdentifier findIdentifier(String column, String operation) {
		
		for (Entry<String, DatasetIdentifier> entry : datasetIdentifiers.entrySet()) {
            if (entry.getValue().getValueAttribute().equals(column) && operation.equals(myAssistant.decodeOperation(entry.getValue().getOperation()))) {
            	return entry.getValue();
            }
        }
	
		return null;
	}
	
	/**
	 * Finds the internal identifier used for the data of a given attribute and its supported operation.
	 * @param column  The name of the attribute.
	 * @param operation  The identifier of the operation (defined in class Constants) supported for this attribute.
	 * @return  The internal dataset identifier.
	 */
	private DatasetIdentifier findIdentifier(String column, int operation) {
		
		for (Entry<String, DatasetIdentifier> entry : datasetIdentifiers.entrySet()) {
            if (entry.getValue().getValueAttribute().equals(column) && entry.getValue().getOperation() == operation) {
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
	 * Identifies whether a data source (to a directory of a JDBC connection to a database) of an attribute is already defined.
	 * @param key  A unique identifier of the data source, usually specified by the administrator.
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
	 * Mounting stage: Determining the data sources (CSV files or JDBC connections) of the attributes to be used in searching.
	 * This method accepts a file with the configuration settings.
	 * @param jsonFile  Path to the JSON configuration file of the data sources, attributes and operations to be used in subsequent search requests.
	 * @return  Notification regarding any issues occurred during mounting of the specified attribute data sources.
	 */
	public Response mount(String jsonFile) {
		
		JSONObject config = parseConfig(jsonFile);		
		return this.mount(config);
	}
	
	
	/**
	 * Mounting stage: Determining the data sources (CSV files or JDBC connections) of the attributes to be used in searching.
	 * This method accepts a configuration formatted as a JSON object.
	 * @param mountConfig  JSON configuration file of the data sources, attributes and operations to be used in subsequent search requests.
	 * @return  Notification regarding any issues occurred during mounting of the specified attribute data sources.
	 */
	public Response mount(JSONObject mountConfig) { 
		
		MountRequest params = null;
		ObjectMapper mapper = new ObjectMapper();	
		try {
			params = mapper.readValue(mountConfig.toJSONString(), MountRequest.class);
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		
		return mount(params);
	}
	
	
	/**
	 * Mounting stage: Determining the data sources (CSV files or JDBC connections) of the attributes to be used in searching.
	 * This method accepts a configuration formatted as a JSON object.
	 * @param params  Parameters specifying the data sources, attributes and operations to be used in subsequent search requests.
	 * @return  Notification regarding any issues occurred during mounting of the specified attribute data sources.
	 */
	public Response mount(MountRequest params) {
		
		Response mountResponse = new Response();
		
		// Instantiate log file and settings
		try {
			Date creation_date = new Date();
			String provided_name = "SimSearch" + new SimpleDateFormat("yyyyMMddHHmmss").format(creation_date);
			if (log == null) {   // Use already existing log file if an index after query execution has started
				String logFile;
				if (params.log != null) 
					logFile = params.log;   // User-specified file location
				else   // Otherwise, use current system time to compose the name of the log file and put it in the TMP directory
					logFile = System.getProperty("java.io.tmpdir") + "/" + provided_name + ".log";
				System.out.println("Logging activity at file:" + logFile);
				log = new Logger(logFile, false);		
			}
			// Specify main settings
			if (instanceSettings == null) {
				instanceSettings = new InstanceSettings();
				instanceSettings.settings.index.setProvidedName(provided_name);
				instanceSettings.settings.index.setCreationDate(String.valueOf(Instant.now().toEpochMilli()));
				// Specify the maximum number of results for individual (single-attribute) queries returned by another REST SimSearch service
				instanceSettings.settings.index.setMaxResultWindow(String.valueOf(Constants.K_MAX * Constants.INFLATION_FACTOR));
				// Initialize the query timeout (in milliseconds)
				instanceSettings.settings.index.setQueryTimeout(Constants.RANKING_MAX_TIME);
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		// Array of available data sources (directories or JDBC connections or REST APIs)
		MountSource[] sources = params.sources;
		
		// JDBC connections opened during mounting
		List<JdbcConnector> openJdbcConnections = new ArrayList<JdbcConnector>();
		
		if (sources != null) {
			
			log.writeln("********************** Mounting data sources ... **********************");
			// Iterate over the specified directories or JDBC connections or REST APIs
			for (MountSource sourceConfig: sources) {

	        	DataSource dataSource = null;
	        	
	        	if (!dataSources.isEmpty())	        	
	        		dataSource = findDataSource(sourceConfig.name);
       	
	        	if (dataSource == null) {
	        		
		        	if (sourceConfig.type.equals("csv")) {   		// This is a CSV data source
		        		if (sourceConfig.url != null)	// File resides in a remote HTTP/FTP server
		        			dataSource = new DataSource(sourceConfig.name, sourceConfig.url);
		        		else							// File resides in a local directory
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
						if (jdbcConnPool.getConnectionPool() != null) {	
							dataSource = new DataSource(sourceConfig.name, jdbcConnPool);
							dataSources.put(dataSource.getKey(), dataSource);
						}
						else {
							String msg = "Data source " + sourceConfig.name + " does not seem able to provide any results.";
							mountResponse.appendNotification(msg + " Check if this DBMS is accessible and the connection details you have specified are correct in the configuration. ");
							log.writeln(msg);
							continue;
						}
		        	}
		        	else if (sourceConfig.type.equals("restapi")) {   // This is a REST API data source, e.g., Elasticsearch	        		
		        		try {
		        			HttpRestConnector httpConn = null;
		        			// Instantiate a connection to the REST API depending on the type of authentication
		        			if (sourceConfig.api_key != null)  // If authorization (API KEY) is specified by the user, then use it 'as is' in the header of requests
		        				httpConn = new HttpRestConnector(new URI(sourceConfig.url), sourceConfig.api_key);
		        			else if ((sourceConfig.username != null) && (sourceConfig.password != null))  // Basic authentication with username/password
		        				httpConn = new HttpRestConnector(new URI(sourceConfig.url), sourceConfig.username, sourceConfig.password);
		        			else  // No authentication required
		        				httpConn = new HttpRestConnector(new URI(sourceConfig.url));
			        			
							// Remember this connection; this may be used for successive queries
							if (httpConn != null) {	
								
								// Check if REST API is responsive
								if (httpConn.getMaxResultCount() > 0) {
									dataSource = new DataSource(sourceConfig.name, httpConn);
									
									// Include type specification for safe discrimination between REST API services
									// Detect whether this REST API connects to another SimSearch instance
									if (httpConn.isSimSearchInstance())
										dataSource.setSimSearchService(true);
									
									dataSources.put(dataSource.getKey(), dataSource);
								}
								else {
									String msg = "Data source " + sourceConfig.name + " does not seem able to provide any results.";
									mountResponse.appendNotification(msg + " Check if this REST API is responsive and the connection details you have specified are correct in the configuration.");
									log.writeln(msg);
									continue;
								}
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
		
		// Keep the metrics specified per attribute for pivot-based search 
		Map<String, String> pivotMetrics = new HashMap<String, String>();
		
		// Create the dictionary of data sources (i.e., queryable attributes) available for search
		if (searchSpecs != null) {

			for (MountSpecs searchConfig: searchSpecs) {

				// operation
				String operation = searchConfig.operation;				
				
				// Check if this dataset should be transformed
				boolean transform = (searchConfig.transform_by != null) && (!searchConfig.transform_by.isEmpty());
				
				// Check if this dataset was the result of a transformation (performed offline)
				boolean transformed = (searchConfig.transformed_to != null) && (!searchConfig.transformed_to.isEmpty());
				
				// Path to directory or name of JDBC connection or REST API
				String sourceId = searchConfig.source;
				
				// Dataset: CSV file or DBMS table or REST API service
				String dataset = searchConfig.dataset;

				// Column to search; its name is used to identify the search operation against its data
				int colQuery = Constants.SEARCH_COLUMN;
				String colValueName = (searchConfig.search_column != null) ? searchConfig.search_column.toString() : dataset + "_" + "colSearch";
				
				if (!dataSources.containsKey(sourceId)) {
					String msg = "Data source with name " + sourceId + " cannot be accessed for attribute " + colValueName + " and will be ignored during search.";
					mountResponse.appendNotification(msg);
					log.writeln(msg);
					continue;
				}

				String sampleValue = null;
				JdbcConnector jdbcConn = null;
				HttpRestConnector httpConn = dataSources.get(sourceId).getHttpConn();
				if (dataSources.get(sourceId).getJdbcConnPool() != null) {   // Querying a DBMS
					jdbcConn = dataSources.get(sourceId).getJdbcConnPool().initDbConnector();   // Required for identifying columns during the mounting phase
					openJdbcConnections.add(jdbcConn); 
					colQuery = jdbcConn.getColumnNumber(dataset, colValueName);
					if (colQuery < 0) {
						String msg = "Attribute name " + colValueName + " is not found in the input data! No queries can target this attribute.";
						mountResponse.appendNotification(msg);
						log.writeln(msg);
						continue;
					}
					else {
						log.writeln("Established connection to " + dataSources.get(sourceId).getJdbcConnPool().getDbSystem() + " for data column " + colValueName);
						sampleValue = jdbcConn.getSampleValue(dataset, colValueName).toString();
					}
				}
				else if (httpConn != null) {   // Querying a REST API 
					httpConn.setSimSearchInstance(dataSources.get(sourceId).isSimSearchService());
					URI origURI = httpConn.uri;	
					// Open a connection
					if (httpConn.isSimSearchInstance()) {  // Catalog request to SimSearch
						try {
							String uriCatalogRequest = origURI.toString().replace("/search", "/catalog");
							httpConn.openConnection(new URI(uriCatalogRequest));
						} catch (URISyntaxException e) {
							e.printStackTrace();
						}  
					}
					else
						httpConn.openConnection();
					// Check if attribute is available in the REST service
					if (!httpConn.fieldExists(colValueName)) {
						String msg = "Attribute name " + colValueName + " is not found in the input data! No queries can target this attribute.";
						mountResponse.appendNotification(msg);
						log.writeln(msg);
						continue;
					}
					else {
						log.writeln("Connections to REST API " + dataSources.get(sourceId).getKey() + " available for queries. Max number of results per request: " + dataSources.get(sourceId).getHttpConn().getMaxResultCount());
						sampleValue = httpConn.getSampleValue(colValueName).toString();
					}
					// Close connection
					if (httpConn.isSimSearchInstance())
						httpConn.closeConnection(origURI);  // IMPORTANT! Restore URI to be used for search
					else			
					    httpConn.closeConnection();
				}
				else if (dataSources.get(sourceId).getPathDir() != null) {   // Otherwise, querying a CSV file
					// File may reside either at the local file system or at a remote HTTP/FTP server
					// CAUTION! Directory (or URL) and file name get concatenated
					File sourcePath = new File(dataSources.get(sourceId).getPathDir());
					if (sourcePath.isDirectory()) {  	// File at a local directory
						File filePath = new File(sourcePath, dataset);
						dataset = filePath.toString();
					}
					else {								// File at a remote server
						try {
							URL url = new URL(dataSources.get(sourceId).getPathDir());
							if (url.getHost() != null)
								dataset = url.toString() + dataset;
						} catch (MalformedURLException e) {
							e.printStackTrace();
						}
					}
									
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
								String msg = "Attribute name " + colValueName + " is not found in the input data! No queries can target this attribute.";
								mountResponse.appendNotification(msg);
								log.writeln(msg);
								continue;
							}
						}
					}				
				}
				else {
					String msg = "Incomplete specifications to allow search against data source " + sourceId;
					mountResponse.appendNotification(msg + ". Please check again your configuration.");
					log.writeln(msg);
					continue;
				}					

				// DatasetIdentifier to be used for all constructs built for this attribute
				DatasetIdentifier id = new DatasetIdentifier(dataSources.get(sourceId), dataset, colValueName, operation, transform);
				
				// Specify whether this dataset can be involved in similarity search queries
				if (searchConfig.queryable != null)
					id.setQueryable(searchConfig.queryable);

				// Assign a sample value for in-situ datasets
				if (sampleValue != null) 
					id.setSampleValue(sampleValue);
				
				// Specify an optional prefix to be used for entity identifiers in the results
				if (searchConfig.prefixURL != null)
					id.setPrefixURL(searchConfig.prefixURL);
				
				// If an ALIAS is specified for the quaryable attribute of an ingested dataset, keep it in the identifier instead of the original name(s)
				if (!id.getDataSource().isInSitu() && (searchConfig.alias_column != null))
					id.setValueAttribute(searchConfig.alias_column);   
				else  // otherwise, use the original attribute name
					id.setValueAttribute(colValueName);				
				
				// If specified, also keep the name of the key attribute in the identifier
				String colKeyName = null;
				if (searchConfig.key_column != null)
					colKeyName = searchConfig.key_column;
				id.setKeyAttribute(colKeyName);  				
				
				// Skip index construction if an index is already built on this attribute
				if (existsIdentifier(id)) {
					String msg = "Attribute " + id.getValueAttribute() + " in dataset " + dataset +  " for " + operation + " has already been defined. Superfluous specification will be ignored.";
					mountResponse.appendNotification(msg);
					log.writeln(msg);
					continue;
				}
				
				// Retain type of operation for the identifier
				switch(operation.toLowerCase()) {
				case "categorical_topk":
					id.setOperation(Constants.CATEGORICAL_TOPK);
					id.setDatatype(DataType.Type.KEYWORD_SET);  // ArrayOfKeywords
					break;
				case "numerical_topk":
					id.setOperation(Constants.NUMERICAL_TOPK);
					id.setDatatype(DataType.Type.NUMBER);  // Number
					break;
				case "spatial_knn":
					id.setOperation(Constants.SPATIAL_KNN);
					id.setDatatype(DataType.Type.GEOLOCATION);  // Geolocation
					break;
				case "temporal_topk":
					id.setOperation(Constants.TEMPORAL_TOPK);
					id.setDatatype(DataType.Type.DATE_TIME);  // Date/time
					break;
				case "name_dictionary":
					id.setOperation(Constants.NAME_DICTIONARY);
					id.setDatatype(DataType.Type.STRING);  // String
					break;
				case "keyword_dictionary":
					id.setOperation(Constants.KEYWORD_DICTIONARY);
					id.setDatatype(DataType.Type.KEYWORD_SET);  //ArrayOfKeywords
					break;
				case "vector_dictionary":
					id.setOperation(Constants.VECTOR_DICTIONARY);
					id.setDatatype(DataType.Type.NUMBER_ARRAY);   // ArrayOfNumbers
					break;
				case "textual_topk":
					id.setOperation(Constants.TEXTUAL_TOPK);
					id.setDatatype(DataType.Type.STRING);  // String
					break;
				case "pivot_based":
					id.setOperation(Constants.PIVOT_BASED);	
					// Data type will be determined after inspecting the attribute data
					id.setDatatype(DataType.Type.UNKNOWN);
					pivotMetrics.put(id.getValueAttribute(), searchConfig.metric);
					break;
		        default:
		        	mountResponse.appendNotification("Unknown operation specified: " + operation);
		        	log.writeln("Unknown operation specified: " + operation);
		        	continue;
		        }
				
				// DATA INGESTION
				// Target dataset is a CSV file, so it must be indexed according to the specifications
				if (dataSources.get(sourceId).getPathDir() != null) {
					index(searchConfig, id, null);
				}
				else if (dataSources.get(sourceId).getHttpConn() != null) {    // REST API
					log.writeln("Data on " + colValueName + " from REST API is not ingested but will be queried directly.");
				}
				else if ((jdbcConn != null)    // Target is a DBMS table, but NOT indexed on this particular attribute OR used as a name dictionary
					&& (!jdbcConn.isJDBCColumnIndexed(dataset, colValueName) || (id.getOperation() == Constants.NAME_DICTIONARY))) {	
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
				
				
				// In case a transformation is required, apply it on the original data and keep the transformed values as a separate (yet associated) dataset
				// Specify the optional vocabulary dataset to be used for transformation (e.g., a set of keywords to a vector of numerical values)
				if (transform) {
					
					long duration = System.nanoTime();
					
					// The vocabulary in the previously defined dictionary
					DatasetIdentifier dictID = findIdentifier(searchConfig.transform_by, Constants.VECTOR_DICTIONARY);
					Map<String, double[]> dictData = (Map<String, double[]>) datasets.get(dictID.getHashKey());
					
					// Define a transformer based on this dictionary
					Word2VectorTransformer transformer = new Word2VectorTransformer(dictData, dictData.values().stream().findFirst().get().length);
					
					// Create a new dataset identifier for the transformed data
					TransformedDatasetIdentifier tranformedID = new TransformedDatasetIdentifier(dataSources.get(sourceId), dataset, colValueName, operation, false);
					tranformedID.setTransformer(transformer);
					tranformedID.setOriginal(id);
					
					// CAUTION! Use the transformed dataset in query evaluation; the original data is only used as a dictionary
					tranformedID.setOperation(id.getOperation());
					id.setOperation(Constants.KEYWORD_DICTIONARY);
				
					// CAUTION! Display the same sample value as in the original dataset
					tranformedID.setSampleValue(id.getSampleValue());

					// Apply the transformation and keep the transformed data in memory for use in similarity search
					// FIXME: Assuming that only attributes with keywords are involved in such transformations
					datasets.put(tranformedID.getHashKey(), transformer.apply((Map<String, String[]>) datasets.get(id.getHashKey())));
					
					// Keep a reference to the attribute identifier of the transformed data
					datasetIdentifiers.put(tranformedID.getHashKey(), tranformedID);			
					
					duration = System.nanoTime() - duration;
					log.writeln("Transformed to vector in " + duration / 1000000000.0 + " sec. In total, " + transformer.numMissingKeywords + " keyword appearances could not be found in the vocabulary.");

					// Associate the transformed dataset with the original one
					id.setTransformed(tranformedID);
				}
				
				// In case this attribute data has been already been transformed offline, associate it with its transformed dataset
				// FIXME: This transformed dataset must be explicitly defined in the config
				if (transformed) {
					// Find the transformed dataset, assuming this is used in PIVOT-based similarity search
					DatasetIdentifier datasetID = findIdentifier(searchConfig.transformed_to, Constants.PIVOT_BASED);
					if (datasetID != null) {
						int op = datasetID.getOperation();

						// Recreate it as a transformed dataset, according to the config
						TransformedDatasetIdentifier tranformedID = new TransformedDatasetIdentifier(datasetID.getDataSource(), datasetID.getDatasetName(), datasetID.getValueAttribute(), myAssistant.decodeOperation(op), false);
						// Associate the transformed dataset with the original one
						id.setTransformed(tranformedID);
						tranformedID.setOperation(op);
						tranformedID.setOriginal(id);
						
						// However, no transformer is known, since transformation has been performed offline
						tranformedID.setTransformer(null);
	
						// Keep the original hashkey, but ...
						// ... replace the reference to the attribute identifier of the transformed data
						datasetIdentifiers.put(datasetID.getHashKey(), tranformedID);
					}
					else {
						String msg = "Transformed dataset " + searchConfig.transformed_to + " was not found.";
						mountResponse.appendNotification(msg);
						log.writeln(msg);
					}
				}
				
				// Keep a reference to the attribute identifier
				datasetIdentifiers.put(id.getHashKey(), id);
	        }	        
		}
		
		// If pivot-based search has been specified, create a multi-dimensional RR*-tree for the concerned attributes
		if (!pivotMetrics.isEmpty()) {
		
			log.writeln("**************Reading data to be used in RR*-tree construction****************");
			
			// Data identifiers specifically used in pivot-based search
			Map<String, DatasetIdentifier> pivotDataIdentifiers = new HashMap<String, DatasetIdentifier>();

		    // Collect a separate dictionary of point values per attribute
		    Map<String, Map<String, Point>> records = new TreeMap<String, Map<String, Point>>();
		    
		    // List of unique identifiers of input entities
		    Set<String> objIdentifiers = new TreeSet<String>();
			for (String key:datasetIdentifiers.keySet()) {
				if (datasetIdentifiers.get(key).getOperation() == Constants.PIVOT_BASED) {
					objIdentifiers.addAll((Collection<? extends String>) datasets.get(key).keySet());
				}
			}
			
			// Number of ordinates in the point representation per attribute
			Map<String, Integer> dimensions = new HashMap<String, Integer>();
			
			// Instantiate a parser for the various types of attribute values
			QueryValueParser valParser = new QueryValueParser();
			
		    // Collect all attribute data required for the construction of pivot-based RR*-tree
		    // CAUTION! If data per attribute comes from different files, must merge all object identifiers and collect their corresponding values 
			for (String key:datasetIdentifiers.keySet()) {
				if (datasetIdentifiers.get(key).getOperation() == Constants.PIVOT_BASED) {
					
					// Identifier of the dataset to be used in pivot-based indexing
					// If this dataset has been transformed, it must use the transformed representation 
					final String datasetId = (datasetIdentifiers.get(key).getTransformed() != null) ? datasetIdentifiers.get(key).getTransformed().getHashKey() : key;

					// Must examine actual attribute data in order to determine the type of each one
					if (datasetIdentifiers.get(datasetId).getDatatype() == Type.UNKNOWN) {
						// Parse a single attribute value from the dataset as indicative of its data type
						Object val = ((TreeMap<String, Point>) datasets.get(datasetId)).firstEntry().getValue();
						valParser.parse(val.toString());
						datasetIdentifiers.get(datasetId).setDatatype(valParser.getDataType());
//						System.out.println(val.toString() + " -> " + valParser.getDataType());
					}
					
					// Keep this identifier for reference
					pivotDataIdentifiers.put(datasetId, datasetIdentifiers.get(datasetId));
					
					// Number of ordinates per point for this attribute in the original dataset
					int d = ((TreeMap<String, Point>) datasets.get(datasetId)).firstEntry().getValue().dimensions();
/*					
					for (Map.Entry<String, Point> e: ((TreeMap<String, Point>) datasets.get(datasetId)).entrySet()) {
						if (e.getValue() != null) {
							d = e.getValue().dimensions();
							break;
						}
					}
*/
					dimensions.put(datasetIdentifiers.get(datasetId).getValueAttribute(), d);
			
					// All NULL values for missing identifiers, are represented with NaN-valued points
					// Find the identifiers of the missing objects
					Set<String> missingKeys = new TreeSet<String>(objIdentifiers);
					missingKeys.removeAll(datasets.get(datasetId).keySet());
					// Create extra dataset with NaN-valued points for the missing identifiers
					if (missingKeys.size() > 0) {
						log.writeln(missingKeys.size() + " object identifiers missing in data for attribute " + datasetIdentifiers.get(datasetId).getValueAttribute() + ". Their value will be filled with points having NaN ordinates.");
	
						TreeMap<String, Point> extra = new TreeMap<String, Point>();
						// Point with NaN values in all ordinates for this attribute
						Point p = myAssistant.createNaNPoint(d);
						for (String missKey: missingKeys) {
							extra.put(missKey, p);
						}
						// Merge the extra dataset to the original
						extra.forEach((k, v) -> ((TreeMap<String, Point>) datasets.get(datasetId)).merge(k, v, (v1, v2) -> Point.create()));
					}
										
					// The merged dataset is used for pivot-based indexing
					records.put(datasetIdentifiers.get(datasetId).getValueAttribute(), (TreeMap<String, Point>) datasets.get(datasetId));
				}
			}			
	
			// Total number of pivot values can be user-specified --> dimensionality of RR*-tree
			int N = Constants.NUM_PIVOTS;   
			if ((params.numPivots != null) && (params.numPivots instanceof Integer)) {
				// RR*-tree can index at least 2-dimensional points 
				N = (params.numPivots < 2) ? Constants.NUM_PIVOTS : params.numPivots;		
				if (params.numPivots < 2) {
					String msg = "Total number of pivots must be an integer greater than 1. Applying default value: " + Constants.NUM_PIVOTS;
					mountResponse.appendNotification(msg);
					log.writeln(msg);
				}
			}
			
			// Instantiate a pivot manager that will be used to create an RR*-tree and support multi-metric similarity search queries
			pivotManager = new PivotManager(N, pivotDataIdentifiers, datasetIdentifiers, datasets, log);				
			
		    // Using ordinal number of attributes involved in pivot-based search
		    MetricReferences ref = new MetricReferences(dataIngestor.getPivotAttrs().size());
			// CAUTION! Metric references MUST have the same ordering as the attribute names!
			int i = 0;
			for (String attr: records.keySet()) {			
				ref.setAttribute(i, attr);  				// Attribute name
				ref.setDimension(i, dimensions.get(attr));  // Dimensionality of point representation per attribute
				ref.setMetric(i, pivotMetrics.get(attr));  	// Metric to be applied on values of this attribute
				i++;
			}

			// FIXME: Should token delimiter be specific per attribute?
			pivotManager.index(ref, Constants.TOKEN_DELIMITER, records);		
		}
		
		// Close any database connections no longer required during the mounting stage
		for (JdbcConnector jdbcConn: openJdbcConnections)
			jdbcConn.closeConnection();
		
		// In case of no errors, notify accordingly
		if (mountResponse.getNotification() == null) {
			mountResponse.appendNotification("Specified data source(s) have been mounted successfully and are available for similarity search queries.");
		}
		
		return mountResponse;
	}

	
	/**
	 * Indexing stage: Ingest data, build all indices and keep in-memory arrays of (key,value) pairs for all indexed attributes.
	 * This method accepts a configuration formatted as a JSON object.
	 * @param attrConfig  Specifications for reading attribute values and creating indices to be used in subsequent search requests.
	 * @param id  Identifier of the dataset containing attribute values.
	 * @param jdbcConn  JDBC connection details for querying data in-situ from a DBMS.
	 */
	public void index(MountSpecs attrConfig, DatasetIdentifier id, JdbcConnector jdbcConn) {
				
		// Re-use existing data ingestor if additional indices need be constructed on-the-fly during query execution
		if (dataIngestor == null)
			dataIngestor = new DataIngestor(log);
		
		// If data comes from a REST API, no indexing is required
		if (id.getDataSource().getHttpConn() != null)
			return;
		
		dataIngestor.proc(attrConfig, id, jdbcConn);
		
		// Update reference to the attribute data identifier, as it may also include the data type
		datasetIdentifiers.put(id.getHashKey(), id);
			
		datasets = dataIngestor.getDatasets();
		indices = dataIngestor.getIndices();
		normalizations = dataIngestor.getNormalizations();			
	}
	
	
	/**
	 * Discard all structures (indices, in-memory look-ups) created on the given attribute(s) according to user-specified configurations.
	 * @param jsonFile   Path to the JSON configuration file of the attributes and operations to be removed.
	 * @return  Notification regarding the removed attribute(s) or any issues occurred during their removal.
	 */
	public Response delete(String jsonFile) {
		
		JSONObject config = parseConfig(jsonFile);		
		return this.delete(config);
	}
	

	/**
	 * Discard all structures (indices, in-memory look-ups) created on the given attribute(s) according to user-specified configurations.
	 * @param removeConfig  JSON configuration for attributes and operations to be removed; these must have been previously specified in a mount request.
	 * @return  Notification regarding the removed attribute(s) or any issues occurred during their removal.
	 */
	public Response delete(JSONObject removeConfig) {
		
		RemoveRequest params = null;
		ObjectMapper mapper = new ObjectMapper();	
		try {
			params = mapper.readValue(removeConfig.toJSONString(), RemoveRequest.class);
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		
		return delete(params);
	}
	
	
	/**
	 * Discard all structures (indices, in-memory look-ups) created on a given attribute according to user-specified parameters.
	 * @param params  Parameters specifying the attributes and operations to be removed; these must have been previously specified in a mount request.
	 * @return  Notification regarding the removed attribute(s) or any issues occurred during their removal.
	 */
	public Response delete(RemoveRequest params) {
		
		Response delResponse = new Response();
		boolean delPivot = false;
		
		// Array of specified data attributes and their supported operations to remove
		AttributeInfo[] arrAttrs2Remove = params.remove;
		try {
			if (arrAttrs2Remove != null) {
				for (AttributeInfo it: arrAttrs2Remove) {
	
					// operation
					String operation = it.getOperation();
					
					// attribute in an input dataset
					String colValueName = it.getColumn();					
					
					//DatasetIdentifier used for all constructs built for this attribute
					DatasetIdentifier id = findIdentifier(colValueName, operation);				
			
					if (id == null) {
						String msg = "No dataset with attribute " + colValueName + " is available for " + operation + " search.";
						log.writeln(msg);
						delResponse.appendNotification(msg);
						throw new NullPointerException(msg);
					}
	
					// If this operation is already supported on this attribute, then remove it
					if (existsIdentifier(id) && (operation.equals(myAssistant.decodeOperation(id.getOperation())))) {
						// Remove all references to constructs on this attribute data using its previously assigned hash key
						removeAttribute(id.getHashKey());
						datasetIdentifiers.remove(id.getHashKey());
						String msg = "Removed support for attribute " + id.getValueAttribute() + " from dataset " + id.getDatasetName() + " in " + operation + " operations.";
						delResponse.appendNotification(msg);
						log.writeln(msg);
						
						// The pivot index should be dropped if at least one of the removed attributes is involved
						delPivot = delPivot || (id.getOperation() == Constants.PIVOT_BASED);
					}
		        }
			}
		} catch (Exception e) {
			String msg = "Attribute removal failed.";
			delResponse.appendNotification(msg + " Please check the specifications in your request.");
			log.writeln(msg);				
		} finally {
			// Destroy pivot manager and all related dataset identifiers 
			if (delPivot) {
				// One instance of RR*-tree may exist, so all pivot-related attributes used in this index must be purged
				for(Iterator<Map.Entry<String, DatasetIdentifier>> it = datasetIdentifiers.entrySet().iterator(); it.hasNext(); ) {
				    Map.Entry<String, DatasetIdentifier> entry = it.next();
				    if (entry.getValue().getOperation() == Constants.PIVOT_BASED) {
				    	removeAttribute(entry.getValue().getHashKey());
				    	it.remove();
				    }
				}
				
				pivotManager = null;
				this.dataIngestor.removeAllPivotAttrs();
				String msg = "RR*-tree index will be no longer available for pivot-based similarity search. All related attribute data have been purged.";
				delResponse.appendNotification(msg);
				log.writeln(msg);
			}
		}
		
		return delResponse;
	}

	/**
	 * Remove all references to constructs on this attribute data identified by its hash key.
	 * @param hashKey  The hash key internally assigned for an attribute dataset.
	 */
	private void removeAttribute(String hashKey) {
		datasets.remove(hashKey);
		indices.remove(hashKey);
		normalizations.remove(hashKey);
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
				dataSources[i] = new AttributeInfo(id.getValueAttribute(), myAssistant.decodeOperation(id.getOperation()), id.getDatatype(), id.getSampleValue(), !id.getDataSource().isInSitu());
				i++;
			}
		}
		
		log.writeln("Listed available data sources.");
		
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
	 * @param catalogConfig  JSON configuration that specifies a CatalogRequest operation.
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
	 * @param params  An instance of a CatalogRequest class; if not null, it should specify a string for similarity search operation (categorical_topk, spatial_knn, numerical_topk, temporal_topk) or a column name.
	 * @return  An array of attribute names along with the supported similarity search operation(s).
	 */
	public AttributeInfo[] listDataSources(CatalogRequest params) {
		
		String operation = params.operation;
		String column = params.column;
		
		// If no specific operation or column is given, report all attributes available for search
		if ((operation == null) && (column == null))
			return listDataSources();
		
		List<AttributeInfo> dataSources = new ArrayList<AttributeInfo>();
		
		// If a particular operation is specified, report only those attributes supporting this operation
		if (operation != null) {	
			for (DatasetIdentifier id: this.datasetIdentifiers.values()) {
				if (operation.equalsIgnoreCase(myAssistant.decodeOperation(id.getOperation()))) {
					dataSources.add(new AttributeInfo(id.getValueAttribute(), myAssistant.decodeOperation(id.getOperation()), id.getDatatype(), id.getSampleValue(), !id.getDataSource().isInSitu()));
				}
			}
		}
		else if (column != null) {  // A column is specified, so report its details
			for (DatasetIdentifier id: this.datasetIdentifiers.values()) {
				if (column.equalsIgnoreCase(id.getValueAttribute())) {
					dataSources.add(new AttributeInfo(id.getValueAttribute(), myAssistant.decodeOperation(id.getOperation()), id.getDatatype(), id.getSampleValue(), !id.getDataSource().isInSitu()));
				}
			}
		}
		
		log.writeln("Listed available data sources.");
		
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
	 * @param searchConfig   JSON configuration that provides the multi-attribute query specifications.
	 * @return   A JSON-formatted response with the ranked results.
	 */
	public SearchResponse[] search(JSONObject searchConfig) {

		SearchRequest params = new SearchRequest();
		
		ObjectMapper mapper = new ObjectMapper();
		try {
			params = mapper.readValue(searchConfig.toJSONString(), SearchRequest.class);
		} catch (JsonProcessingException e) {
			System.out.println(e.getCause());
//			e.printStackTrace();
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

		try {
			log.writeln("********************** New search request ... **********************");
			// The same instance of pivot manager handles all incoming pivot-based similarity search requests
			if ((params.algorithm != null) && (params.algorithm.equals("pivot_based") && (pivotManager != null))) {
				pivotManager.setCollectQueryStats(this.isCollectQueryStats());	// Specify whether to collect detailed query statistics
				return pivotManager.search(params);
			}
			else {		// A new handler is created for each request involving rank aggregation
				SearchHandler reqHandler = new SearchHandler(dataSources, datasetIdentifiers, datasets, indices, normalizations, log);
				reqHandler.setCollectQueryStats(this.isCollectQueryStats());	// Specify whether to collect detailed query statistics
				return reqHandler.search(params, this.instanceSettings.settings.index.getQueryTimeout());
			}
		} catch (Exception e) {
			e.printStackTrace();
			SearchResponse[] responses = new SearchResponse[1];
			SearchResponse response = new SearchResponse();
			String msg = "Search request discarded due to illegal specification of query attributes or parameters. " + e.getMessage();
			log.writeln(msg);
			response.setNotification(msg + " Please check your query specifications.");
			responses[0] = response;
			return responses;
		}
	}
	
	
	/**
	 * Explicitly writes the given message to the log file of the running instance.
	 * @param msg  A message to be written to the log file.
	 */
	public void log(String msg) {	
		if (this.log != null)
			this.log.writeln(msg);
	}


	/**
	 * Provides the main settings of the running SimSearch instance.
	 * @return  An object that contains generic informations about this instance.
	 */
	public InstanceSettings getSettings() {
		return instanceSettings;
	}
	
	/**
	 * Indicates whether the platform is empirically evaluated and collects execution statistics.
	 * @return  True, if collecting execution statistics; otherwise, False.
	 */
	public boolean isCollectQueryStats() {
		return collectQueryStats;
	}


	/**
	 * Specifies whether the platform will be collecting execution statistics when running performance tests. 
	 * @param collectQueryStats  True, if collecting execution statistics; otherwise, False.
	 */
	public void setCollectQueryStats(boolean collectQueryStats) {
		this.collectQueryStats = collectQueryStats;
	}
	
}
