package eu.smartdatalake.simsearch.csv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.locationtech.jts.geom.Geometry;

import com.google.common.base.Joiner;

import eu.smartdatalake.simsearch.Assistant;
import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.csv.categorical.InvertedIndex;
import eu.smartdatalake.simsearch.csv.categorical.TokenSetCollection;
import eu.smartdatalake.simsearch.csv.categorical.TokenSetCollectionReader;
import eu.smartdatalake.simsearch.csv.lookup.DictionaryReader;
import eu.smartdatalake.simsearch.csv.numerical.BPlusTree;
import eu.smartdatalake.simsearch.csv.numerical.DoubleNumReader;
import eu.smartdatalake.simsearch.csv.numerical.INormal;
import eu.smartdatalake.simsearch.csv.numerical.UnityNormal;
import eu.smartdatalake.simsearch.csv.numerical.ZNormal;
import eu.smartdatalake.simsearch.csv.spatial.Location;
import eu.smartdatalake.simsearch.csv.spatial.LocationReader;
import eu.smartdatalake.simsearch.csv.spatial.RTree;
import eu.smartdatalake.simsearch.jdbc.JdbcConnector;
import eu.smartdatalake.simsearch.manager.DatasetIdentifier;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Point;
import eu.smartdatalake.simsearch.request.MountSpecs;

/**
 * Ingests the values of an attribute from a dataset according to user's configuration.
 * Also builds indices: Inverted Index (for categorical search); B+-tree (for numerical search); R-tree (for spatial search).
 * For pivot-based similarity search, no index is built here; it only collects dictionaries of attribute values that will be processed by the PivotManager.
 */
public class DataIngestor {

	Logger log = null;
	Map<String, Map<?,?>> datasets = null;
	Map<String, Index> indices = null;
	Map<String, INormal> normalizations = null;
	List<String> pivotAttrs = null;    // For pivot-based search, keep the attributes involved
	
	Assistant myAssistant;
	
	/**
	 * Constructor
	 * @param log  Handle to the log file for notifications and execution statistics.
	 */
	public DataIngestor(Logger log) {
		
		this.log = log;	
		this.datasets = new HashMap<String, Map<?,?>>();
		this.indices = new HashMap<String, Index>();
		this.normalizations = new HashMap<String, INormal>();
		this.pivotAttrs = new ArrayList<String>();
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

		// Check if this dataset to be transformed
		boolean transformed = (mountConfig.transform_by != null) && (!mountConfig.transform_by.isEmpty());
		
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
		List<Integer> colValues = new ArrayList<Integer>();
		String colValueName = null;
		// Check whether multiple columns have been specified; FIXME: currently, only considering this for lon, lat coordinates in locations
		if (mountConfig.search_column != null) {
			if (mountConfig.search_column instanceof ArrayList) {
				// Iterate over all columns and get their ordinal numbers in the attribute schema
				for (Object col: (Iterable<?>) mountConfig.search_column) {
					colValue = myAssistant.getColumnNumber(dataset, col.toString(), columnSeparator);
					if (colValue < 0) {
						log.writeln("Attribute name " + col.toString() + " is not found in the input data! No index will be built on this attribute.");
						return;
					}
					colValues.add(colValue);
				}
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
				else {  // Column name cannot be identified without a header
					colValue = -1;   // This is used in reading dictionaries from CSV files with arrays of values per row
				}
			}
		}

		// Determine the type of index according to the type of similarity search query
		// No index created in case this attribute is used only as a dictionary of keywords
		if ((operation.equalsIgnoreCase("categorical_topk")) || (operation.equalsIgnoreCase("keyword_dictionary"))){

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
			
			if (jdbcConn != null) {	// Input comes from a non-indexed column in a database table acquired via a JDBC connection
				// Always specify the key column as well!
				targetCollection = reader.ingestFromJDBCTable(id.getDatasetName(), colKeyName, id.getValueAttribute(), tokenDelimiter, jdbcConn, log);
				log.writeln("Ingested data from JDBC data source on column " + id.getValueAttribute());
			}
			else {					// Input comes from a CSV file
				targetCollection = reader.importFromCSVFile(dataset, colKey, colValue, columnSeparator, tokenDelimiter, maxLines, header, log);
			}
			
			// Use a generated hash key of the column as a reference to the collected values for this attribute
			datasets.put(id.getHashKey(), targetCollection.sets);						
		
			duration = System.nanoTime() - duration;
			
			// If used as a dictionary, then this attribute data is not queryable
			if (operation.equalsIgnoreCase("keyword_dictionary")) {
				id.setQueryable(false);
				log.writeln("Dictionary on " + id.getValueAttribute() + " created in " + duration / 1000000000.0 + " sec.");
			}
			
			// Build indices for queryable attributes only
			if (id.isQueryable()) {
				log.writeln("Read time: " + duration / 1000000000.0 + " sec.");
				
				// Build inverted index against the target data (tokens)
				InvertedIndex index = reader.buildInvertedIndex(targetCollection, log);
				// Use the generated hash key as a reference to the index built on this attribute
				indices.put(id.getHashKey(), index);
	
				//CAUTION! No normalization applied to categorical search queries
				normalizations.put(id.getHashKey(), null);
				
				log.writeln("Index on " + id.getValueAttribute() + " created.");
			}
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
			DoubleNumReader doubleNumReader = new DoubleNumReader();
			INormal normal = null;				
			Map<String, Double> targetData = null;
			
			if (jdbcConn != null) {	// Input comes from a non-indexed column in a database table acquired via a JDBC connection
				// Always specify the key column as well!
				targetData = doubleNumReader.readFromJDBCTable(id.getDatasetName(), colKeyName, id.getValueAttribute(), jdbcConn, log);
				log.writeln("Ingested data from JDBC data source on column " + id.getValueAttribute());
			}
			else {					// Input comes from a CSV file
				targetData = doubleNumReader.readFromCSVFile(dataset, maxLines, colValue, colKey, columnSeparator, header, log);
			}
			
			// Use the generated hash key as a reference to the collected values for this attribute
			datasets.put(id.getHashKey(), targetData);
			
			// Build indices for queryable attributes only
			if (id.isQueryable()) {
			
				BPlusTree<Double, String> index = null;
				// Apply normalization (if specified) against input dataset
				if ((normalized != null) && (normalized.equalsIgnoreCase("z"))) {
					normal = new ZNormal(doubleNumReader.avgVal, doubleNumReader.stDev);
					index = doubleNumReader.buildNormalizedIndex(targetData, normal, log);
				}
				else if ((normalized != null) && (normalized.equalsIgnoreCase("unity"))) {
					normal = new UnityNormal(doubleNumReader.avgVal, doubleNumReader.minVal, doubleNumReader.maxVal);
					index = doubleNumReader.buildNormalizedIndex(targetData, normal, log);
				}
				else
					index = doubleNumReader.buildIndex(targetData, log);
	
				// Use the generated hash key as a reference to the index built on this attribute
				indices.put(id.getHashKey(), index);
					
				// Remember the kind of normalization applied against this dataset
				normalizations.put(id.getHashKey(), normal);
				
				duration = System.nanoTime() - duration;
				log.writeln("Index on " + id.getValueAttribute() + " created in " + duration / 1000000000.0 + " sec.");
				log.writeln("Index contains " + index.numNodes + " internal nodes and " + index.numLeaves + " leaf nodes.");	
			}
		}
		// settings for k-NN similarity search on spatial locations
		else if (operation.equalsIgnoreCase("spatial_knn")) {
			
			// INDEX BUILDING
			duration = System.nanoTime();
			
			// Consume specific columns from input file and ...
			// ...build B+-tree on the chosen (key,value) pairs
			LocationReader locReader = new LocationReader();				
			Map<String, Geometry> targetData;
			if (mountConfig.search_column instanceof ArrayList) {
				// Two coordinate values specified for (POINT) locations
				String[] colCoords = colValueName.split(",");    // CAUTION! "," is the character signifying that multiple columns are involved
				if (jdbcConn != null) {	// Input comes from a non-indexed column in a DBMS table acquired via a JDBC connection
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
					// Preferably, use WKT representations for geometries in a single column of the data
				targetData = locReader.readFromCSVFile(dataset, maxLines, colKey, colValue, columnSeparator, header, log);
				// TODO: Ingest location data when it comes from a single non-indexed column in a DBMS acquired via a JDBC connection
			}
			
			// Use the generated hash key as a reference to the collected values for this attribute
			datasets.put(id.getHashKey(), targetData);
			
			// Build indices for queryable attributes only
			if (id.isQueryable()) {
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
		}
		// settings for creating a lookup dictionary of names (NOT used in similarity search query evaluation)
		else if (operation.equalsIgnoreCase("name_dictionary")) {
			
			// DATA INGESTION
			duration = System.nanoTime();
			
			// Consume specific columns from input file and ...
			// ...build a lookup dictionary on the chosen (key,value) pairs
			DictionaryReader<String, String> stringValReader = new DictionaryReader<String, String>();			
			Map<String, String> targetData = null;
			
			if (jdbcConn != null) {	// Input comes from a column in a database table acquired via a JDBC connection
				// Always specify the key column as well!
				targetData = stringValReader.readFromJDBCTable(id.getDatasetName(), colKeyName, id.getValueAttribute(), jdbcConn, log);
				log.writeln("Ingested data from JDBC data source on column " + id.getValueAttribute());
			}
			else {					// Input comes from a CSV file
				targetData = stringValReader.readFromCSVFile(dataset, maxLines, colKey, colValue, columnSeparator, header, log);
			}
			
			// By default, dictionaries are NOT queryable in similarity search; no index is built
			id.setQueryable(false);
			
			// Use the generated hash key as a reference to the collected values for this attribute
			datasets.put(id.getHashKey(), targetData);
			
			duration = System.nanoTime() - duration;
			log.writeln("Dictionary on " + id.getValueAttribute() + " created in " + duration / 1000000000.0 + " sec.");
		}
		// settings for creating a lookup dictionary of arrays of string values (to be used in PIVOT-based similarity search query evaluation)
		else if (operation.equalsIgnoreCase("pivot_based") && transformed) {
			
			// DATA INGESTION
			duration = System.nanoTime();
			
			// Delimiter between tokens (keywords)
			String tokenDelimiter = Constants.TOKEN_DELIMITER;
			if (mountConfig.token_delimiter != null) {
				tokenDelimiter = mountConfig.token_delimiter;
				if (tokenDelimiter == null || tokenDelimiter.equals(""))
					tokenDelimiter = Constants.TOKEN_DELIMITER;		
			}	
			
			// Consume specific columns from input file and ...
			// ...build a lookup dictionary on the chosen (key,value) pairs
			// FIXME: Assuming that the vectors consist of string values
			DictionaryReader<String, String[]> dictReader = new DictionaryReader<String, String[]>(tokenDelimiter, String.class);			
			Map<String, String[]> targetData = null;
			
			if (jdbcConn != null) {	// Input comes from a column in a database table acquired via a JDBC connection
				// Always specify the key column as well!
				targetData = dictReader.readFromJDBCTable(id.getDatasetName(), colKeyName, id.getValueAttribute(), jdbcConn, log);
				log.writeln("Ingested data from JDBC data source on column " + id.getValueAttribute());
			}
			else {					// Input comes from a CSV file
				targetData = dictReader.readFromCSVFile(dataset, maxLines, colKey, colValue, columnSeparator, header, log);
			}
			
			// Keep the name of the attribute, not its ordinal number
			pivotAttrs.add(colValueName);
						
			// By default, dictionaries are NOT queryable in similarity search; no index is built
			id.setQueryable(false);
		
			// Use the generated hash key as a reference to the collected values for this attribute
			datasets.put(id.getHashKey(), targetData);
	
			duration = System.nanoTime() - duration;
			log.writeln("Dictionary on " + id.getValueAttribute() + " created in " + duration / 1000000000.0 + " sec.");
		}
		// settings for creating a lookup dictionary of arrays of double values (to be used in PIVOT-based similarity search query evaluation)
		else if (operation.equalsIgnoreCase("vector_dictionary")) {
			
			// DATA INGESTION
			duration = System.nanoTime();
			
			// Delimiter between tokens (keywords)
			String tokenDelimiter = Constants.TOKEN_DELIMITER;
			if (mountConfig.token_delimiter != null) {
				tokenDelimiter = mountConfig.token_delimiter;
				if (tokenDelimiter == null || tokenDelimiter.equals(""))
					tokenDelimiter = Constants.TOKEN_DELIMITER;		
			}	
			
			// Consume specific columns from input file and ...
			// ...build a lookup dictionary on the chosen (key,value) pairs
			// FIXME: Assuming that the vectors consist of double values
			DictionaryReader<String, double[]> dictReader = new DictionaryReader<String, double[]>(tokenDelimiter, Double.class);			
			Map<String, double[]> targetData = null;
			
			if (jdbcConn != null) {	// Input comes from a column in a database table acquired via a JDBC connection
				// Always specify the key column as well!
				targetData = dictReader.readFromJDBCTable(id.getDatasetName(), colKeyName, id.getValueAttribute(), jdbcConn, log);
				log.writeln("Ingested data from JDBC data source on column " + id.getValueAttribute());
			}
			else {					// Input comes from a CSV file
				targetData = dictReader.readFromCSVFile(dataset, maxLines, colKey, colValue, columnSeparator, header, log);
			}
			
			// By default, dictionaries are NOT queryable in similarity search; no index is built
			id.setQueryable(false);
		
			// Use the generated hash key as a reference to the collected values for this attribute
			datasets.put(id.getHashKey(), targetData);
	
			duration = System.nanoTime() - duration;
			log.writeln("Dictionary on " + id.getValueAttribute() + " created in " + duration / 1000000000.0 + " sec.");
		}
		// No special index is built per attribute for pivot-based search; returned dictionaries of attribute values must be collected and converted to multi-dimensional points 
		else if (operation.equalsIgnoreCase("pivot_based")) {

			// Delimiter between tokens (keywords)
			String tokenDelimiter = Constants.TOKEN_DELIMITER;
			if (mountConfig.token_delimiter != null) {
				tokenDelimiter = mountConfig.token_delimiter;
				if (tokenDelimiter == null || tokenDelimiter.equals(""))
					tokenDelimiter = Constants.TOKEN_DELIMITER;		
			}
		
			duration = System.nanoTime();
			MetricDataReader reader = new MetricDataReader();				
			Map<String, Point> targetData = null;
			// TODO:  Handle data from a non-indexed column in a DBMS acquired via a JDBC connection
			if (jdbcConn != null) {	// Input comes from a non-indexed column in a database table
				// Must always specify the key column as well!
//				targetData = reader.ingestFromJDBCTable(id.getDatasetName(), colKeyName, id.getValueAttribute(), tokenDelimiter, jdbcConn, log);
//				log.writeln("Ingested data from JDBC data source on column " + id.getValueAttribute());
			}
			else {					// Input comes from a CSV file
				if (colValues.size() > 1)   // Property value constructed from multiple columns
					targetData = reader.ingestFromCSVFile(dataset, colKey, colValues.toArray(new Integer[0]), columnSeparator, tokenDelimiter, maxLines, header, log);
				else
					targetData = reader.ingestFromCSVFile(dataset, colKey, colValue, columnSeparator, tokenDelimiter, maxLines, header, log);
			}
			
			// Use a generated hash key of the column as a reference to the collected values for this attribute
			datasets.put(id.getHashKey(), targetData);						
		
			// Keep the name of the attribute, not its ordinal number
			pivotAttrs.add(colValueName);
			
			duration = System.nanoTime() - duration;
			log.writeln("Read time: " + duration / 1000000000.0 + " sec.");			
			
			// No special indices for pivot-based attributes
			// Use the generated hash key as a reference
			if (id.isQueryable()) {

				// No special index in built on a single attribute data
				indices.put(id.getHashKey(), null);
	
				//CAUTION! No normalization applied to metric data
				normalizations.put(id.getHashKey(), null);
			}
		}
		//TODO: Include indexing for other types of operations...
		else {
			log.writeln("Unknown operation specified: " + operation);
		}

	}
	        
	// GETTER methods for all created structures  

	public Map<String, Map<?,?>> getDatasets() {
		return datasets;
	}
	
	public Map<String, Index> getIndices() {
		return indices;
	}
	
	public Map<String, INormal> getNormalizations() {
		return normalizations;
	}
	
	public List<String> getPivotAttrs() {
		return pivotAttrs;
	}

	public void removeAllPivotAttrs() {
		pivotAttrs.clear();
	}
	
}
