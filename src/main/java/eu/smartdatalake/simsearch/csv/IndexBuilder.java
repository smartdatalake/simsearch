package eu.smartdatalake.simsearch.csv;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.locationtech.jts.geom.Geometry;

import com.google.common.base.Joiner;

import eu.smartdatalake.simsearch.Assistant;
import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.DatasetIdentifier;
import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.csv.categorical.InvertedIndex;
import eu.smartdatalake.simsearch.csv.categorical.TokenSetCollection;
import eu.smartdatalake.simsearch.csv.categorical.TokenSetCollectionReader;
import eu.smartdatalake.simsearch.csv.numerical.BPlusTree;
import eu.smartdatalake.simsearch.csv.numerical.DataReader;
import eu.smartdatalake.simsearch.csv.numerical.INormal;
import eu.smartdatalake.simsearch.csv.numerical.UnityNormal;
import eu.smartdatalake.simsearch.csv.numerical.ZNormal;
import eu.smartdatalake.simsearch.csv.spatial.Location;
import eu.smartdatalake.simsearch.csv.spatial.LocationReader;
import eu.smartdatalake.simsearch.csv.spatial.RTree;
import eu.smartdatalake.simsearch.jdbc.JdbcConnector;
import eu.smartdatalake.simsearch.request.MountSpecs;

/**
 * Builds the specified indices over the values of an attribute from a dataset according to user's configuration.
 * Supported indices: Inverted Index (for categorical search); B+-tree (for numerical search); R-tree (for spatial search).
 */
public class IndexBuilder {

	Logger log = null;
	Map<String, HashMap<?,?>> datasets = null;
	Map<String, Index> indices = null;
	Map<String, INormal> normalizations = null;
	
	Assistant myAssistant;
	
	/**
	 * Constructor
	 * @param log  Handle to the log file for notifications and execution statistics.
	 */
	public IndexBuilder(Logger log) {
		
		this.log = log;	
		this.datasets = new HashMap<String, HashMap<?,?>>();
		this.indices = new HashMap<String, Index>();
		this.normalizations = new HashMap<String, INormal>();
		myAssistant = new Assistant();
	}

	
	/**
	 * Builds an index for the identified dataset according to user-specified configurations.
	 * @param mountConfig  Configuration for reading the attribute values and constructing an index prior to query execution.
	 * @param id  Identifier of the dataset on which an index will be built.
	 * @param jdbcConn  Specifications for the JDBC connection to be used for retrieving attribute values and constructing an in-memory index.
	 */
	public void proc(MountSpecs mountConfig, DatasetIdentifier id, JdbcConnector jdbcConn) {
		
		long duration;

		// operation
		String operation = mountConfig.operation;

		// file parsing
		String columnSeparator = Constants.COLUMN_SEPARATOR;
		if (mountConfig.separator != null) {
			columnSeparator = mountConfig.separator;
			if (columnSeparator == null || columnSeparator.equals(""))
				columnSeparator = Constants.COLUMN_SEPARATOR;
		}
		boolean header = false;
		if (mountConfig.header != null)
			header = mountConfig.header;
		
		int maxLines = -1;
		if (mountConfig.max_lines != null)
			maxLines = mountConfig.max_lines;
		
		// input dataset
		String dataset = id.getDatasetName();
		
		// Specification of the column containing datasetIdentifiers
		int colKey = Constants.KEY_COLUMN;   // Default value with datasetIdentifiers
		String colKeyName = null;
		if (mountConfig.key_column != null) {
			colKeyName = mountConfig.key_column;
			if (jdbcConn != null)   						// JDBC source
				colKey = myAssistant.getColumnNumber(dataset, colKeyName, jdbcConn);
			else if (id.getDataSource().getHttpConn() == null) 	// CSV source
				colKey = myAssistant.getColumnNumber(dataset, colKeyName, columnSeparator);
			if (colKey < 0) {
				log.writeln("Attribute name " + colKeyName + " is not found in the input data! No index can be built with this key column.");
				return;
			}
			else if (jdbcConn == null)
				header = true;   // Expect that header exists in the input CSV dataset
		}
		
		// Specification of the column containing values
		int colValue = Constants.SEARCH_COLUMN;  // Default column with values to search against
		String colValueName = null;
		// Check whether multiple columns have been specified; FIXME: currently, only considering this for lon, lat coordinates
		if (mountConfig.search_column != null) {
			if (mountConfig.search_column instanceof ArrayList) {
				// Combine name for the virtual column to be used for indexing and searching				
				colValueName = Joiner.on(",").join((Iterable<?>) mountConfig.search_column);
				header = true;   // Header MUST exist in the file in order to identify the columns specified
			}
			else {   // A single column has been specified
				colValueName = mountConfig.search_column.toString();
				if (header) {     // If header exists in the CSV file, identify the column
					colValue = myAssistant.getColumnNumber(dataset, colValueName, columnSeparator);
					if (colValue < 0) {
						log.writeln("Attribute name " + colValueName + " is not found in the input data! No index will be built on this attribute.");
						return;
					}
				}
			}
		}

		// Determine the type of index according to the type of similarity search query
		if (operation.equalsIgnoreCase("categorical_topk")) {

			// Delimiter between tokens (keywords)
			String tokenDelimiter = Constants.TOKEN_DELIMITER;
			if (mountConfig.token_delimiter != null) {
				tokenDelimiter = mountConfig.token_delimiter;
				if (tokenDelimiter == null || tokenDelimiter.equals(""))
					tokenDelimiter = Constants.TOKEN_DELIMITER;		
			}				

			TokenSetCollectionReader reader = new TokenSetCollectionReader();
			TokenSetCollection targetCollection;			
			duration = System.nanoTime();
			
			if (jdbcConn != null) {	// Input comes from a non-indexed column in a database table
				// TODO: Always specify the key column as well!
				targetCollection = reader.ingestFromJDBCTable(id.getDatasetName(), colKeyName, id.getValueAttribute(), tokenDelimiter, jdbcConn, log);
				log.writeln("Ingested data from JDBC data source on column " + id.getValueAttribute());
			}
			else {					// Input comes from a CSV file
				targetCollection = reader.importFromCSVFile(dataset, colKey, colValue, columnSeparator, tokenDelimiter, maxLines, header, log);
			}
			
			// Use a generated hash key of the column as a reference to the collected values for this attribute
			datasets.put(id.getHashKey(), targetCollection.sets);						
		
			duration = System.nanoTime() - duration;
			log.writeln("Read time: " + duration / 1000000000.0 + " sec.");
			
			// TODO:  Handle data from a non-indexed column in a DBMS acquired via a JDBC connection
			
			// Build inverted index against the target data (tokens)
			InvertedIndex index = reader.buildInvertedIndex(targetCollection, log);
			// Use the generated hash key as a reference to the index built on this attribute
			indices.put(id.getHashKey(), index);

			//CAUTION! No normalization applied to categorical search queries
			normalizations.put(id.getHashKey(), null);
			
			log.writeln("Index on " + id.getValueAttribute() + " created.");
		}
		// settings for top-k similarity search on numerical values
		else if (operation.equalsIgnoreCase("numerical_topk")) {
			
			// CAUTION! Numeric values in the data are used as KEYS in the B+-tree index
			//...and datasetIdentifiers in the data are considered as VALUES in the index

			// Determines whether numeric values should be normalized
			String normalized = null;
			if (mountConfig.normalized != null)
				normalized = mountConfig.normalized;
			
			// INDEX BUILDING
			duration = System.nanoTime();
			
			// Consume specific columns from input file and ...
			// ...build B+-tree on the chosen (key,value) pairs
			DataReader dataReader = new DataReader();
			INormal normal = null;				
			HashMap<String, Double> targetData = null;
			
			if (jdbcConn != null) {	// Input comes from a non-indexed column in a database table
				// TODO: Always specify the key column as well!
				targetData = dataReader.readFromJDBCTable(id.getDatasetName(), colKeyName, id.getValueAttribute(), jdbcConn, log);
				log.writeln("Ingested data from JDBC data source on column " + id.getValueAttribute());
			}
			else {					// Input comes from a CSV file
				targetData = dataReader.readFromCSVFile(dataset, maxLines, colValue, colKey, columnSeparator, header, log);
			}
			
			// Use the generated hash key as a reference to the collected values for this attribute
			datasets.put(id.getHashKey(), targetData);
			
			BPlusTree<Double, String> index = null;
			// Apply normalization (if specified) against input dataset
			if ((normalized != null) && (normalized.equalsIgnoreCase("z"))) {
				normal = new ZNormal(dataReader.avgVal, dataReader.stDev);
				index = dataReader.buildNormalizedIndex(targetData, normal, log);
			}
			else if ((normalized != null) && (normalized.equalsIgnoreCase("unity"))) {
				normal = new UnityNormal(dataReader.avgVal, dataReader.minVal, dataReader.maxVal);
				index = dataReader.buildNormalizedIndex(targetData, normal, log);
			}
			else
				index = dataReader.buildIndex(targetData, log);

			// Use the generated hash key as a reference to the index built on this attribute
			indices.put(id.getHashKey(), index);
				
			// Remember the kind of normalization applied against this dataset
			normalizations.put(id.getHashKey(), normal);
			
			duration = System.nanoTime() - duration;
			log.writeln("Index on " + id.getValueAttribute() + " created in " + duration / 1000000000.0 + " sec.");
			log.writeln("Index contains " + index.numNodes + " internal nodes and " + index.numLeaves + " leaf nodes.");				
		
		}
		// settings for k-NN similarity search on spatial locations
		else if (operation.equalsIgnoreCase("spatial_knn")) {
			
			// INDEX BUILDING
			duration = System.nanoTime();
			
			// Consume specific columns from input file and ...
			// ...build B+-tree on the chosen (key,value) pairs
			LocationReader locReader = new LocationReader();				
			HashMap<String, Geometry> targetData;
			if (mountConfig.search_column instanceof ArrayList) {
				// Two coordinate values specified for (POINT) locations
				String[] colCoords = colValueName.split(",");    // CAUTION! "," is the character signifying that multiple columns are involved
				if (jdbcConn != null) {	// Input comes from a non-indexed column in a DBMS table
					// Must always specify the key column in the configuration
					targetData = locReader.readFromJDBCTable(id.getDatasetName(), colKeyName, colCoords[0], colCoords[1], jdbcConn, log);
					log.writeln("Ingested location data from JDBC data source on columns " + colCoords[0] + ", " + colCoords[1]);
				}
				else {					// Input comes from a CSV file
					int colLongitude = myAssistant.getColumnNumber(dataset, colCoords[0], columnSeparator);
					int colLatitude = myAssistant.getColumnNumber(dataset, colCoords[1], columnSeparator);
					colValue = colLongitude; 	// By default, mark the column for the longitude
					targetData = locReader.readFromCSVFile(dataset, maxLines, colKey, colLongitude, colLatitude, columnSeparator, header, log);
				}
			}		
			else {  // By default, a single column should used for referencing to this index; 
					// preferably, use WKT representations for geometries in a single column of the data
				targetData = locReader.readFromCSVFile(dataset, maxLines, colKey, colValue, columnSeparator, header, log);
				// TODO: Ingest location data when it comes from a single non-indexed column in a DBMS acquired via a JDBC connection
			}
			
			// Use the generated hash key as a reference to the collected values for this attribute
			datasets.put(id.getHashKey(), targetData);
			
			// Create the R-tree index on this data
			RTree<String, Location> index = locReader.buildIndex(targetData, log);
			
			// Use the generated hash key as a reference to the index built on this attribute
			indices.put(id.getHashKey(), index);
				
			// NO normalization is applied against spatial datasets
			normalizations.put(id.getHashKey(), null);
			
			duration = System.nanoTime() - duration;
			log.writeln("Index on " + id.getValueAttribute() + " created in " + duration / 1000000000.0 + " sec.");
			log.writeln("MBR: " + index.getMBR().toString() + ". Index has " + index.getDepth() + " levels and contains " + index.getItems() + " object locations.");				
			
		}
		//TODO: Include indexing for other types of operations...
		else {
			log.writeln("Unknown operation specified: " + operation);
		}

	}
	        
	// GETTER methods for all created structures  

	public Map<String, HashMap<?,?>> getDatasets() {
		return datasets;
	}
	
	public Map<String, Index> getIndices() {
		return indices;
	}
	
	public Map<String, INormal> getNormalizations() {
		return normalizations;
	}

}
