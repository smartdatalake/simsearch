package eu.smartdatalake.simsearch.pivoting;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

import eu.smartdatalake.simsearch.request.SearchRequest;
import eu.smartdatalake.simsearch.request.SearchSpecs;
import eu.smartdatalake.simsearch.Assistant;
import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.engine.IResult;
import eu.smartdatalake.simsearch.engine.OutputWriter;
import eu.smartdatalake.simsearch.engine.QueryValueParser;
import eu.smartdatalake.simsearch.engine.SearchResponse;
import eu.smartdatalake.simsearch.engine.SearchResponseFormat;
import eu.smartdatalake.simsearch.engine.processor.RankedResult;
import eu.smartdatalake.simsearch.engine.processor.ResultFacet;
import eu.smartdatalake.simsearch.engine.weights.Estimator;
import eu.smartdatalake.simsearch.engine.weights.Validator;
import eu.smartdatalake.simsearch.manager.DataType.Type;
import eu.smartdatalake.simsearch.manager.ingested.lookup.Word2VectorTransformer;
import eu.smartdatalake.simsearch.manager.DataType;
import eu.smartdatalake.simsearch.manager.DatasetIdentifier;
import eu.smartdatalake.simsearch.manager.TransformedDatasetIdentifier;
import eu.smartdatalake.simsearch.pivoting.rtree.Entry;
import eu.smartdatalake.simsearch.pivoting.rtree.MultiMetricSimilaritySearch;
import eu.smartdatalake.simsearch.pivoting.rtree.NearestEntry;
import eu.smartdatalake.simsearch.pivoting.rtree.RTree;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Point;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Rectangle;

/**
 * Creates a pivot-based, multi-dimensional RR*-tree and then handles multi-attribute similarity search requests. 
 * CAUTION! A single instance of this class is created by the coordinator.
 */
public class PivotManager {

	Logger log = null;
	Assistant myAssistant;

	static RTree<Object, Point> tree; 	// Instantiation of an RR*-tree index
	private int M = 0;  				// Total number of distances (i.e., queryable attributes)
	private int R;   					// Total number of reference (pivot) values --> This is the admin-specified dimensionality of the RR*-tree

	MetricReferences ref;
	List<List<Point>> pivots;
	
	String delimiter;
	
	// Input datasets and their identifiers used in creating the index and for reporting results
	Map<String, Map<?,?>> datasets;
	Map<String, DatasetIdentifier> pivotDataIdentifiers;
	Map<String, DatasetIdentifier> datasetIdentifiers;     // All dataset identifiers, in case they include names (non-queryable attribute)
	String[] attrIdentifiers;
	
	// Dictionary holding information per attribute used in estimating similarity scores with exponential decay function
	Map<String, MetricSimilarity> metricSimilarities;
	
	// Sample values per attribute collected for estimations
	Map<String, List<Point>> samples;
	
	// Fixed scaling factors determined during tree construction; alternatively, they can be computed dynamically at query time
	ScaleFactors scaleFactors;
	
	// Estimator to auto-configure weights for attribute(s) in case of no user-specified values 
	Estimator estimator;
	
	long duration;

	private boolean collectQueryStats;
	
	// Keep track of any word2vec transformers associated with particular attributes
	Map<String, Word2VectorTransformer> transformers;

	
	/**
	 * Constructor
	 * @param r  Total number of reference values (pivots).
	 * @param pivotDataIdentifiers  Dictionary of the attributes available for PIVOT-based similarity search operations.
	 * @param datasetIdentifiers  Dictionary of the attributes available for similarity search operations.
	 * @param datasets  Dictionary of the attribute datasets available for querying.
	 * @param log  Handle to the log file for notifications and execution statistics.
	 */
	public PivotManager(int r, Map<String, DatasetIdentifier> pivotDataIdentifiers, Map<String, DatasetIdentifier> datasetIdentifiers,  Map<String, Map<?, ?>> datasets, Logger log) {

		this.datasets = datasets;
		this.pivotDataIdentifiers = pivotDataIdentifiers;
		this.datasetIdentifiers = datasetIdentifiers;
		this.log = log;	
		this.R = r;
		myAssistant = new Assistant();
		
		// Keep track of any word2vec transformers defined for particular attributes
		transformers = new HashMap<String, Word2VectorTransformer>();	
		for (Map.Entry<String, DatasetIdentifier> entry : datasetIdentifiers.entrySet()) {
			if (entry.getValue().isTransformed()) {
				TransformedDatasetIdentifier datasetId = (TransformedDatasetIdentifier)entry.getValue();
				transformers.put(datasetId.getValueAttribute(), datasetId.getTransformer());
			}		
		}
	}

	
	/**
	 * Finds the internal identifier used for the data of a given attribute.
	 * @param column  The name of the attribute.
	 * @return  The internal dataset identifier.
	 */
	private DatasetIdentifier findIdentifier(String column) {
		
		for (Map.Entry<String, DatasetIdentifier> entry : this.datasetIdentifiers.entrySet()) {
			// Only considering operations involved in pivot-based similarity search, 
			// i.e., excluding attributes supported in rank aggregation or those used as dictionaries
			if (entry.getValue().getOperation() == Constants.PIVOT_BASED) {
				if (entry.getValue().getValueAttribute().equals(column))
					return entry.getValue();
            }
        }
	
		return null;
	}

	/**
	 * Exports the embeddings of input objects indexed in the RR*-tree into a CSV file.
	 * @param csvFileName  The name of the output CSV file.
	 * @param embeddings  The collection of the embeddings with an entry per input entity. Object: the identifier; Point: a multi-dimensional point with the embedded ordinate values.
	 */
	public void exportEmbeddings(String csvFileName, List<Entry<Object, Point>> embeddings) {
		
		// Use default column separator for output
		String separator = Constants.COLUMN_SEPARATOR;
		
		PrintWriter writer = null;
		try {
			writer = new PrintWriter(csvFileName, "UTF-8");
			for (Entry<Object, Point> entry: embeddings) {
				// One row per embedded point using the default delimiter
				writer.println(entry.value() + separator + entry.geometry().toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			writer.close();
		}	
	}
	
	
    /**
     * Only used for embedding query points before searching in the index.
     * @param q  M-dimensional query point (one value per attribute)
     * @return  The embedding (i.e., distance values from pivots) of the query point.
     */
	private double[] embed(Map<String, Point> q) {
		
		// R-dimensional vector of reference values (pivots); one or multiple pivots per attribute
		double e[] = new double[R];
	
		int n = 0;  // Number of pivot values currently calculated
		for (String attr: q.keySet()) {		// A different distance may apply per attribute
			int m = ref.getAttributeOrder(attr);  // Metric corresponding to this attribute in the reference distances
			// For each distance, use the relevant pivots to compute distances from the attribute values of the query point
			for (int i = 0; i < ref.countDimensionReferenceValues(m); i++) {
				e[n] = ref.getMetric(m).calc(q.get(attr), pivots.get(m).get(i));	
				n++;
			}		
		}

		return e;				
	}


	/**
	 * Provides a random sample of given size from a list of objects
	 * @param inCol  Input collection of objects of a given type.
	 * @param n  The number of objects to choose.
	 * @return  A randomly chosen subset of the given objects.
	 */
	public <T> List<T> randomSample(Collection<T> inCol, int n) {
		
		// For very small datasets, the sample is the dataset itself
	    if (inCol.size() < n)
	    	return (List<T>) inCol;
	    
		List<T> newCol = new ArrayList<>(inCol);
	    Collections.shuffle(newCol);
	    return newCol.subList(0, n);
	}

	
	/**
	 * Constructs a (multi-dimensional) point from the query value specified for the given attribute. 
	 * @param attr  The queried attribute.
	 * @param datatype  The data type of the query value; should match that of the respective attribute.
	 * @param val  The parsed query value.
	 * @param transform  Boolean specifying whether the query value (set of keywords) should be transformed (into an array of doubles)
	 * @return  A (multi-dimensional) point to be used in the search against the index.
	 */
	private Point constructQueryPoint(String attr, Type datatype, Object val, boolean transform) {
		
    	if (val != null) {
    		if ((datatype == Type.KEYWORD_SET) && (transform)) {
    			// Transform original query value into a vector representation according to the appointed transformer
    			return Point.create(transformers.get(attr).getVector((String[]) val));
    		}
    		else if (datatype == Type.NUMBER_ARRAY) {
    			// Handling array of double values in the query
    			return Point.create(Stream.of((Double[]) val).mapToDouble(Double::doubleValue).toArray());
    		}
    		else if (datatype == Type.DATE_TIME) {
    			// Handling a date/time value has been already converted to epoch (a double number)
    			return Point.create(new double[]{Double.parseDouble(val.toString())});
    		}
    		else if (datatype == Type.GEOLOCATION) {
    			// Handling a query location
    			return Point.create(Arrays.stream((String[]) val).mapToDouble(Double::parseDouble).toArray());
    		}
    		else {
    			// Handling a numerical query value (CAUTION! This is parsed as a string)
    			return Point.create(Arrays.stream((String[]) val).mapToDouble(Double::parseDouble).toArray());	
    		}
    	}

		// If value is not specified, a NaN-valued point will be created for this attribute with a suitable number of ordinates
		log.writeln("Created NaN point for query value at attribute " + attr + ".");
		return myAssistant.createNaNPoint(ref.getDimension(attr));
	
	}
	
	
	/**
	 * Indexing stage: Construct an RR*-tree index based on the input records, using the given distances and determining suitable reference points (i.e., pivots)
	 * @param ref  The distance reference to be used for embedding input records.
	 * @param tokenDelimiter  The delimiter character between coordinates in attribute values
	 * @param records  Input collection of records: (identifier, point) pairs per attribute.
	 * @return  True, if the index has been successfully constructed; otherwise, False.
	 */
	public boolean index(MetricReferences ref, String tokenDelimiter, Map<String, Map<String, Point>> records) {
		
		// Delimiter only used in search
		// Assuming the same delimiter of tokens used for input is also used in query specification
		delimiter = tokenDelimiter;
	
		// Metric references as in the given specifications
		M = records.size(); 	// Number of distances (one per queryable attribute)
		this.ref = ref; 		// array of distance references (one item per attribute)
		
		// Fixed scaling factors determined during tree construction
		scaleFactors = new ScaleFactors(ref, M);
		
		// Samples to be populated from the records
		samples = new HashMap<String,List<Point>>();
		
		try {
			log.writeln("**************RR*-tree: Estimating number of pivots per distance****************");
	    	duration = System.nanoTime();
	    	
	    	// Obtain a random sample of objects per distance
	    	// CAUTION! Depending on the chosen samples, selection of pivots per distance may be affected
	    	List<List<Point>> sample = new ArrayList<List<Point>>(M);		
	    	for (String attr: records.keySet()) {
	    		// CAUTION! Pivot setting should work on a small sample, e.g., 500 objects
	    		// To avoid NULL values, first get a larger sample, e.g. 2-5 times the size of the required one ...
	    		Collection<Point> nonNullSubset = new ArrayList<Point>();
	    		int numRounds = 5;  
	    		while ((numRounds > 0) && (nonNullSubset.size() < Constants.NUM_SAMPLES)) {
		    		List<Point> randomSet = randomSample(records.get(attr).values(), Constants.NUM_SAMPLES);
		    		// ... exclude any NULL values from this sample
		    		nonNullSubset.addAll(randomSet.stream().filter(c -> !c.containsNaN()).collect(Collectors.toList()));
//		    		System.out.println("NOT NULL items: " + nonNullSubset.size());
		    		numRounds--;
	    		}
	    		// ... and finally get the final sample	   
	    		List<Point> subset = randomSample(nonNullSubset, Constants.NUM_SAMPLES);
	    		samples.put(attr, subset);
	    		sample.add(subset);
/*	    		
	    		for (Point p: randomSet) {
	    			System.out.println(p.toString());
	    		}
	    		System.out.println("*************Chosen " + randomSet.size() + " objects for " + m + "-th distance.**********************");
*/	    		
	    	}    	
	    	
	    	// This process does NOT choose the actual pivots, but only a suitable number of pivots per distance
	    	PivotAllocation setting = new PivotAllocation(M, R, ref.metrics, sample, log);
	    	int[] pivotsPerMetric = setting.greedyMaximization();
	    	
	    	// FIXME: Assign the average NN distances per distance as the respective scale factors
	    	scaleFactors.scale = setting.getEpsilonThresholds();
	    	
	    	// Select number of pivots per attribute (distance metric)
	    	int cntPivots = 0;
	    	for (String attr: records.keySet()) {
	    		int m = ref.getAttributeOrder(attr);   // Metric reference corresponding to this attribute
	    		ref.setStartReference(m, cntPivots);
	    		cntPivots += pivotsPerMetric[m];
	    		ref.setEndReference(m, cntPivots - 1);
	    	}		

	    	// For each attribute (distance metric) involved in the index (and the queries), identify its respective data source
	    	attrIdentifiers = new String[ref.countMetrics()];
			for (Map.Entry<String, DatasetIdentifier> id: pivotDataIdentifiers.entrySet()) {
				// Attribute name
				String attr = id.getValue().getValueAttribute();
				// The internal hashkey of each dataset is assigned per distance metric
				attrIdentifiers[ref.getAttributeOrder(attr)] = id.getValue().getHashKey();
			}
			
			duration = System.nanoTime() - duration;
			log.writeln("RR*-tree pivot setting: " + duration / 1000000000.0 + " sec.");
	    		    	
	    	log.writeln("**************RR*-tree: Pivot selection & embedding****************");
	    	duration = System.nanoTime();
	    	
			// Initialize list for pivot values to be determined per attribute (distance metric)
			// CAUTION! Points for different distances may have differing number of ordinates
			pivots = new ArrayList<List<Point>>(M);		
	     	for (int m = 0; m < M; m++) {
	     		pivots.add(new ArrayList<Point>());
	     	}
	    	
	    	// Multi-dimensional array to hold the distances from the chosen pivots
	    	// 1st dimension -> attribute; 2nd dimension -> object; 3rd dimension -> used pivot
	    	double[][][] distances = new double[M][][];
	    	for (String attr: records.keySet()) {
	    		int m = ref.getAttributeOrder(attr);  // Metric reference corresponding to this attribute
	    		// If this metric has been assigned with pivots, choose them 
	    		if (ref.countDimensionReferenceValues(m) > 0) {
		    		// FIXME: Substitute distance values for NaN ordinates with the scale factor used for this attribute
		    		ref.getMetric(m).setNaNdistance(scaleFactors.scale[m]);
		    		log.writeln(ref.countDimensionReferenceValues(m) + " PIVOTS for attribute " + ref.getAttribute(m) + " using " + ref.getMetric(m).getClass().getSimpleName() + " :");
	
		    		// Pivot selection is only based on the values concerning a particular attribute and distance
		    		PivotSelector selector = new PivotSelector(ref.getMetric(m), new ArrayList<Point>(records.get(attr).values()), log);
		    		// Number of pivots to select for this attribute have been estimated
		    		distances[m] = selector.embed(ref.countDimensionReferenceValues(m));   		
		    		// Retain the selected pivots for subsequently embedding query points
		    		pivots.get(m).addAll(selector.getPivots());
	    		}
	    	}
	    	
	    	// Construct array of embeddings per input object
	    	// This will become an R-dimensional point to be indexed in the tree
	    	List<Entry<Object, Point>> points = new ArrayList<Entry<Object, Point>>();
	    	Entry<Object, Point> entry = null; 
	    	int c = 0;
	    	double[] val;  // Array of embeddings for this object
	    	// IMPORTANT! Assuming that entity identifiers are identical for all lists of input records
	    	for (String entityId: records.get(ref.getAttribute(0)).keySet()) {
	    		val = new double[R];
	    		int r = 0;
	    		for (int j = 0; j < M; j++) {   // For each attribute (distance metric)
	    			// Number of pivots may differ per attribute
	    			for (int l = 0; l < ref.countDimensionReferenceValues(j); l++) {
	    				val[r] = distances[j][c][l];	// Embedded distance from pivot
	    				r++;
	    			}
	    		}
	    		entry = Entry.entry(entityId, Point.create(val));
	    		points.add(entry);
//		    	System.out.println(Arrays.toString(val));
	    		c++;
	    	}
	    	
	    	duration = System.nanoTime() - duration;
	    	log.writeln("RR*-tree embedding cost: " + duration / 1000000000.0 + " sec.");
	    	
	    	// Optionally, export the embeddings into a CSV file
//	    	exportEmbeddings("embeddings.csv", points);
	    		    	
	//		System.out.println("*****************RR*-tree construction using pivots **********************");
			//Create the RR*-tree
			duration = System.nanoTime();
			tree = RTree.dimensions(R).maxChildren(Constants.NODE_FANOUT).star().<Object, Point>create(points);
			
	    	// Tree construction statistics 
	    	duration = System.nanoTime() - duration;
	    	log.writeln("RR*-tree construction time: " + duration / 1000000000.0 + " sec.");
	    	log.writeln("Indexed objects: " + tree.size());
	        log.writeln("RR*-tree dimensions: " + tree.dimensions());
	          
	        // Total extent of the entire tree (i.e., the MBR of its root) over all dimensions
	        Optional<Rectangle> opt = tree.mbr();
	        opt.ifPresent(mbr -> log.writeln("RR*-tree extent: " + mbr.toText()));
	      
	        // Report the fixed scaling factors to be used at query time
	        for (String attr: records.keySet()) {
	        	// Identify the distance reference corresponding to this attribute and get its scale factor
	        	log.writeln("Default scale factor for " + attr + " : " + scaleFactors.getAll()[ref.getAttributeOrder(attr)]);   
	        }
	        log.writeln("RR*-tree index is now available to answer pivot-based similarity search queries.");
		}
		catch(Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}

	
	/**
	 * Adding specification for an attribute used in the index, but not specified in the use query.
	 * @param attrName  The name of the missing attribute.
	 * @param weightCombinations  The number of weight combinations specified for other attributes in the query.
	 * @return  An extra query specification for an attribute.
	 */
	private SearchSpecs addExtraQuerySpecs(String attrName, int weightCombinations) {
		
		SearchSpecs extraSpecs = new SearchSpecs();
		extraSpecs.column = attrName;
		extraSpecs.value = null;   // Representing NULL value
		
		// Define zero weights for this attribute		
		Double[] weights = new Double[weightCombinations];
		Arrays.fill(weights, 0.0);
		extraSpecs.weights = weights;
		
		log.writeln("Attribute " + attrName + " was not included in the query specification. A NULL value has been specified with zero weights.");
		
		return extraSpecs;
	}

	
	/**
	 * Constructs a multi-dimensional point per attribute from the user-specified query values.
	 * @param querySpecs  The query specifications per attribute.
	 * @param scale  The scale factors to be applied; user-specified values may be set.
	 * @param attrWeights  The weight combinations per attribute.
	 * @param notification  Message notification to be returned in case of errors.
	 * @return  The query object to be used in searching the index. 
	 */
	private Map<String, Point> setQueryValues(SearchSpecs[] querySpecs, double[] scale, Map<String, Double[]> attrWeights, String notification) {
			
        // CAUTION! Query: a multi-dimensional point must be constructed per attribute
        Map<String, Point> qPoint = new TreeMap<String, Point>();
        
		// Instantiate a parser for the various types of query values
		QueryValueParser valParser = new QueryValueParser(delimiter);

		// Attribute names are associated with their respective values 
		String[] qValues = new String[querySpecs.length];
		String[] qColumns = new String[querySpecs.length];
		DatasetIdentifier datasetId;
		// Multi-dimensional query point as an array of attribute values (one per queried attribute)
		for (int i = 0 ; i < querySpecs.length; i++) {
			// Search column; Multiple attributes (e.g., lon, lat) will be combined into a virtual column [lon, lat] for searching
			String colValueName = querySpecs[i].column.toString();
			// Identify the corresponding dataset
			// Also handles cases where an array of attributes is used (e.g., lon/lat coordinates for location)
			datasetId = this.findIdentifier(colValueName);
			if (datasetId == null) {
				String msg = "Attribute data on " + colValueName + " has not been specified for pivot-based similarity search.";
				notification.concat(msg + " ");
				log.writeln(msg);
				continue;
			}
			
			// Parse user-specified query value for this attribute and 
			qColumns[i] = datasetId.getValueAttribute();
			Object val = null;
			if (datasetId.getDatatype() == Type.DATE_TIME)  // Special handling of date/time values
				val = valParser.parseDate(querySpecs[i].value);
			if (val == null)   // Other data types
				val = valParser.parse(querySpecs[i].value);
			
			// Create a (multi-dimensional) query point
			Point p = constructQueryPoint(qColumns[i], valParser.getDataType(), val, datasetId.needsTransform());
        	qPoint.put(qColumns[i], p);
        	qValues[i] = (querySpecs[i].value != null) ? querySpecs[i].value.toString() : "null";
        	
        	// Check compatibility of data types
        	if (datasetId.getDatatype() != valParser.getDataType())
        		notification.concat("" + "Query value " + String.valueOf(querySpecs[i].value) + " is not of type " + datasetId.getDatatype() + " as the attribute data.");
        	
        	// Retain the weights associated with this attribute
        	attrWeights.put(qColumns[i], querySpecs[i].weights);
        	
        	// Specify the distance similarity to be used for this attribute (distance)
        	int m = ref.getAttributeOrder(qColumns[i]);   // The distance (attribute) involved
        	// Apply the user-specified scale factor for distances on this attribute
        	if (querySpecs[i].scale != null)
    			scale[m] = querySpecs[i].scale;
        	// Also keep a decay factor if specified by the user for this attribute
        	MetricSimilarity metricSimilarity = new MetricSimilarity(p, ref.getMetric(m), ((querySpecs[i].decay != null) ? querySpecs[i].decay : Constants.DECAY_FACTOR), scale[m], m);
        	// As key employ the same hashkey used for the respecive attribute dataset
        	metricSimilarities.put(datasetId.getHashKey(), metricSimilarity);
		}

		return qPoint;
	}
	
	
	/**
	 * Searching stage: Given a user-specified configuration, execute the pivot-based similarity search and issue the ranked top-k results.
	 * This method accepts an instance of SearchRequest class.
	 * @param params   An instance of SearchRequest class with the multi-attribute search query specifications.
	 * @return  A JSON-formatted response with the ranked results.
	 */
	public SearchResponse[] search(SearchRequest params) {
		
		duration = System.nanoTime();  
		
		SearchResponse[] responses;
		String notification = "";  // Any extra notification(s) to the final response

		// Specifications for writing results into an output CSV file (if applicable)
		OutputWriter outCSVWriter = new OutputWriter(params.output);
		
		// Construct for validating weights
		Validator weightValidator = new Validator();
				
		// Query specifications
		// NOTE: Query may not specify all indexed attributes
		SearchSpecs[] querySpecs = params.queries;
		List<String> queryAttributes = new ArrayList<String>();
		for (int i = 0 ; i < querySpecs.length; i++) {
			// Check if attribute was specified more than once
			if (queryAttributes.contains(querySpecs[i].column.toString()))
				log.writeln("Attribute " + querySpecs[i].column + " has been specified more than once. Only the last specification was used in query evaluation.");
			queryAttributes.add(querySpecs[i].column.toString());	
		}
		
		// Initialize number of combinations of weights
        int weightCombinations = 1;
		
		// In case of non-specified weights, create estimators so that they can be assigned dynamically later
		estimator = new Estimator();
		boolean missingWeights = false;   // By default, assume that all weights are specified 		
		for (SearchSpecs queryConfig: querySpecs) {
			if ((queryConfig.weights != null) && (queryConfig.weights.length == 0)) {  // Empty array of weights
				queryConfig.weights = null;
			}
			// Missing weights should be automatically determined from statistical analysis
			if (queryConfig.weights == null) {
				estimator.setMissingWeight(queryConfig.column.toString());
				missingWeights = true;
			}
			else {
				// Validate user-specified weights
				if (!weightValidator.check(String.valueOf(queryConfig.column), queryConfig.weights)) {
					responses = new SearchResponse[1];
					SearchResponse response = new SearchResponse();
					String msg = "Request aborted because at least one weight value for attribute " + String.valueOf(queryConfig.column) + " is invalid.";
					log.writeln(msg);
					response.setNotification(msg + " Weight values must be real numbers strictly between 0 and 1.");
					responses[0] = response;
					return responses;
				}
				// Find combination of weights with max cardinality
				if (queryConfig.weights.length > weightCombinations)
					weightCombinations = queryConfig.weights.length;	
			}
		}
		
		// Keep track of any attributes not involved in the query
        List<String> missingAttributes = new ArrayList<String>();
        
		// Initialize the distance similarity to be used per attribute (metric)
		metricSimilarities = new HashMap<String, MetricSimilarity>();
		for (int i = 0 ; i < ref.countMetrics(); i++) {
			metricSimilarities.put(this.findIdentifier(ref.getAttribute(i)).getHashKey(), null);
			
			// IMPORTANT! Check for missing attributes in the query specs
			if (!queryAttributes.contains(ref.getAttribute(i))) {
				// Remember this attribute in order to skip it when reporting results
				missingAttributes.add(ref.getAttribute(i));   
				// Add extra attributes used in the index, but not specified in the query
				SearchSpecs[] newQuerySpecs = new SearchSpecs[querySpecs.length+1];
				System.arraycopy(querySpecs, 0, newQuerySpecs, 0, querySpecs.length);
				newQuerySpecs[querySpecs.length] = addExtraQuerySpecs(ref.getAttribute(i), weightCombinations);
				querySpecs = newQuerySpecs;
			}		
		}
		
		// top-k query specification
		int topk = params.k;
		
		// Check for excessive top-k value
		if (topk > Constants.K_MAX) {
			responses = new SearchResponse[1];
			SearchResponse response = new SearchResponse();
			log.writeln("Search request discarded, because no more than top-" + Constants.K_MAX + " results can be returned per query.");
			response.setNotification("Please specify a positive integer value up to " + Constants.K_MAX + " for k and submit your request again.");
			responses[0] = response;
			return responses;
		}
					
		// Weights: dictionary with attribute names as keys
    	Map<String, Double[]> attrWeights = new HashMap<String, Double[]>();
    	
    	// Scale factors to be used in this search
    	double[] scale = scaleFactors.getAll();   // Default values to apply if not specified in the query configuration
	
        // Query specification: a multi-dimensional point must be constructed per attribute
    	Map<String, Point> qPoint = setQueryValues(querySpecs, scale, attrWeights, notification);
    			
		// Check whether values have been specified for all queryable attributes
		if (metricSimilarities.values().contains(null)) {
			responses = new SearchResponse[1];
			SearchResponse response = new SearchResponse();
			String msg = "Query value is missing in at least one attribute. Please check your query specification.";
			log.writeln(msg);
			response.setNotification(msg.concat(notification));
			responses[0] = response;
			return responses;		
		}
		
        // Embedded query point with the same pivots in order to be used during tree traversal
        Point q = Point.create(embed(qPoint));
        log.writeln("Query embedding: " + Arrays.toString(q.mins()));
        
        // WEIGHT ESTIMATION
		// Invoke estimation of weight(s) if not specified for some attributes
		if (missingWeights) {
			// Calculate indicative similarity scores based on the sample points
			for (String attr : attrWeights.keySet()) {
				if (estimator.hasMissingWeight(attr)) {
					estimator.setInput(attr, findScoresFromSample(this.findIdentifier(attr).getHashKey(), qPoint.get(attr), samples.get(attr)));
				}
			}
			//estimator.proc();		// Estimate based on standard deviation of scores
			estimator.proc(topk);	// ALTERNATIVE: Estimate derived from percentiles and depends on the top-k parameter
			attrWeights.putAll(estimator.getWeights(weightCombinations));
			// Log estimated weights
			String msg = "Weights assigned: ";
			for (String attr : estimator.getAttributesWithoutWeight())
				msg += attr + " -> " +  estimator.getWeight(attr) + "; ";
			log.writeln(msg);
		}
		
        /********************QUERY EVALUATION ******************/
         
		/********CAUTION! Skipping estimation of scale factors per query at runtime;******** 
		 ********scale factors are assigned during pivot setting in tree construction*******/
/*
		ScaleFactors scaling = new ScaleFactors(ref, M);
        // 1st ALTERNATIVE (NOT USED): Calculate scaling factors to be used in normalizing distance values
        // FIXME: Do these scale factors need be different per weight combination?       
        scaling.setScale2KNN(datasets, attrIdentifiers, tree.root().get(), q, k);
        // 2nd ALTERNATIVE (NOT USED): Apply the maximum range of values for normalization
//      scaling.setScale2MaxRange(tree.mbr().get().mins(), tree.mbr().get().maxes());
*/        

        // For each combination of weights run a new top-k similarity search query
        // Array to collect top-k results per weight combination	
     	IResult[][] allResults = new IResult[weightCombinations][topk];
     	MultiMetricSimilaritySearch simQuery;
     	// Iterate over all weight combinations
        for (int j = 0; j < weightCombinations; j++) {
    	
        	// Construct the array of weights to apply per attribute
	        double[] w = new double[M];
	        int m;
	        for (String attr: attrWeights.keySet()) {
	        	m = ref.getAttributeOrder(attr);   
	        	w[m] = attrWeights.get(attr)[j];  // Use the j-th weight per attribute
	        }
	
	        // Perform a top-k similarity search query with these weights against the multi-dimensional RR*-tree
	        simQuery = new MultiMetricSimilaritySearch(datasets, attrIdentifiers, ref, w, scaleFactors.getAll(), this.log);
	        Iterable<NearestEntry<Object, Point, Double>> simResults = simQuery.search(tree.root().get(), q, qPoint, topk);
			int rank = 1;   // ranking order of issued results
			// Report each result 
			for (NearestEntry<Object, Point, Double> r : simResults) {
				// CAUTION! The RR*-tree returns distance values, not similarity scores
				// This distance is based on pivot embeddings; not on actual distance of this entity from query q
//				allResults[j][rank-1] = issueRankedResult(rank, r.value().toString(), r.distance(), true);	
				allResults[j][rank-1] = issueRankedResult(rank, r.value().toString(), r.distance(), qPoint, w, missingAttributes, true);				
				rank++;
	        }
			// FIXME: Extra sorting step since the tree utilizes distances instead of similarity scores 
			allResults[j] = sortByScore(allResults[j]);
        }
        
		duration = System.nanoTime() - duration;   
    	
		// Format response, including similarity matrix of results pairwise
		SearchResponseFormat responseFormat = new SearchResponseFormat();
		responses = responseFormat.proc(allResults, attrWeights, datasetIdentifiers, datasets, datasets, null, null, metricSimilarities, topk, this.isCollectQueryStats(), notification, outCSVWriter);

		double execTime = duration / 1000000000.0;
		log.writeln("SimSearch [pivot-based] issued " + responses[0].getRankedResults().length + " results. Processing time: " + execTime + " sec.");

		// Execution cost for experimental results; the same time cost concerns all weight combinations
		for (SearchResponse response: responses) { 
			response.setTimeInSeconds(execTime);	
		}
		
		// Close output writer to CSV (if applicable)
		if (outCSVWriter.isSet())
			outCSVWriter.close();
        
		return responses;
	}

	
	/**
	 * Sorts the results by their final similarity score instead of the rankings determined by the pivot-based distances in the index.
	 * @param results  The query results as ranked by the tree index.
	 * @return  The results sorted by descending similarity score.
	 */
	private RankedResult[] sortByScore(IResult[] results) {
		
		int k = results.length;
		RankedResult[] sortedResults = new RankedResult[k];		  
		
		// Use a dictionary to sort results by descending overall score
		// CAUTION! Multiple results may have equal scores
		ListMultimap<Double, Integer> mapScores = Multimaps.newListMultimap(new TreeMap<>(Collections.reverseOrder()), ArrayList::new);	
		for (int i = 0; i < k; i++) {
			mapScores.put(results[i].getScore(), i+1);
		}
		
		// Assign the new rankings to the results
		int rank = 1;
		for (Map.Entry<Double, Integer> entry : mapScores.entries()) {
			sortedResults[rank-1] = (RankedResult) results[entry.getValue()-1];
			sortedResults[rank-1].setRank(rank);
			rank++;	
		}
		
		return sortedResults;
	}
	
	
	/**
	 * Prepares the next result for reporting, along with its original attribute values as used in the raw data.
	 * NOT USED: This variant does not provide similarity scores per attribute.
	 * @param i  The rank of the result in the top-k list.
	 * @param oid  An entity identifier
	 * @param distByIndex  The weighted distance of this result as returned by the index.
	 * @param missingAttributes  The names of the attributes not specified in the user's query.
	 * @param exact  Boolean specifying whether this result is exact (true) or approximate (false).
	 * @return  The next result to report.
	 */
	private RankedResult issueRankedResult(int i, Object oid, double distByIndex, List<String> missingAttributes, boolean exact) {

		// Create a new resulting item and report its rank and its original identifier
		// Includes names and values per individual attribute ...
		RankedResult res = new RankedResult(pivotDataIdentifiers.entrySet().size() - missingAttributes.size());
		res.setId((String) oid);
		res.setRank(i);
		
		// ... also its original values at the searched attributes
		int j = 0;
		for (Map.Entry<String, DatasetIdentifier> id: pivotDataIdentifiers.entrySet()) {		
			// Skip reporting any attribute not specified in the user's query
			if (missingAttributes.contains(id.getValue().getValueAttribute()))
				continue;
			// Otherwise, report its value and its (unweighed) similarity score
			ResultFacet attr = new ResultFacet();
			attr.setName(id.getValue().getValueAttribute());
			attr.setValue(datasets.get(id.getValue().getHashKey()).get(oid).toString());
			attr.setScore(Double.NaN);  // FIXME: No individual scores available per attribute; using NaN to denote its absence
			res.getAttributes()[j] = attr;
			j++;
		}
		
		// From the estimated distance based on pivots compute a similarity score with an exponential decay function
		// FIXME: The default decay factor is used in this computation
		res.setScore(Math.exp(- Constants.DECAY_FACTOR * distByIndex)); 
		
		// Indicate whether this ranking should be considered exact or not
		// Pivot-based results be considered exact, since overall distances are based on exact computations over original attribute values
		res.setExact(exact);

		return res;
	}
	
	
	/**
	 * Prepares the next result for reporting, along with its original attribute values as used in the raw data.
	 * This variant calculates the similarity scores per attribute for each result w.r.t. the query.
	 * @param i  The rank of the result in the top-k list.
	 * @param oid  An entity identifier of the result.
	 * @param distByIndex  The weighted distance of this result as returned by the index.
	 * @param qPoint  The query with its user-specified attribute values.
	 * @param w  The combination of weights to apply per attribute for estimating the similarity score.
	 * @param missingAttributes  The names of the attributes not specified in the user's query.
	 * @param exact  Boolean specifying whether this result is exact (true) or approximate (false).
	 * @return  The next result to report.
	 */
	private RankedResult issueRankedResult(int i, Object oid, double distByIndex, Map<String, Point> qPoint, double[] w, List<String> missingAttributes, boolean exact) {

		double score = 0.0;	
		double sumWeights = Arrays.stream(w).sum();
		 
		// Create a new resulting item and report its rank and its original identifier
		// Includes names and values per individual attribute ...
		RankedResult res = new RankedResult(pivotDataIdentifiers.entrySet().size() - missingAttributes.size());
		res.setId((String) oid);
		// FIXME: Ranking may change because of the exponential decay applied on similarity scores 
		res.setRank(i);
		
		// ... also its original values at the searched attributes
		double s = 0.0;
		int j = 0;
		for (Map.Entry<String, DatasetIdentifier> id: pivotDataIdentifiers.entrySet()) {
			
			// Skip reporting any attribute not specified in the user's query
			if (missingAttributes.contains(id.getValue().getValueAttribute()))
				continue;
						
			// Otherwise, report its value and its (unweighed) similarity score
			ResultFacet attr = getAttrSpecs(id.getValue(), oid);
			
			// Use the respective distance to compute the actual, yet SCALED distance
			// Apply exponential decay to estimate a similarity score on this attribute
			s = metricSimilarities.get(id.getValue().getHashKey()).calc((Point) datasets.get(id.getValue().getHashKey()).get(oid));
			attr.setScore(s);
			
			// Report this attribute value and the estimated score
			res.getAttributes()[j] = attr;
			
			// Update total similarity score with the user-specified weights
			// Get the weight specified for this attribute
			score += w[ref.getAttributeOrder(attr.getName())] * s;
			
			j++;
		}
		
		// Report the weighted similarity score over all attributes as the overall score
		res.setScore(score / sumWeights); 
	
		// ALTERNATIVE: From the estimated distance based on pivots compute a similarity score with an exponential decay function
		// FIXME: The default decay factor is used in this computation
		//res.setScore(Math.exp(- Constants.DECAY_FACTOR * distByIndex)); 
		
		// Indicate whether this ranking should be considered exact or not
		// Pivot-based results be considered exact, since overall distances are based on exact computations over original attribute values
		res.setExact(exact);

		return res;
	}
	
	
	/**
	 * Provides an attribute name and its value for a given entity in a dataset.
	 * @param datasetId  The dataset identifier of the attribute values.
	 * @param oid  The identifier of a given object in the dataset.
	 * @return  The name and value for an attribute of the requested entity to be reported in the results.
	 */
	private ResultFacet getAttrSpecs(DatasetIdentifier datasetId, Object oid) {
		
		ResultFacet attr = new ResultFacet();
		attr.setName(datasetId.getValueAttribute());
		
		// In case an attribute has been transformed, return the original values and not the transformed ones
		Object val;
		if (datasetId.needsTransform()) {
			TransformedDatasetIdentifier id = (TransformedDatasetIdentifier)datasetId;
			val = datasets.get(id.getOriginal().getHashKey()).get(oid);
		}
		// Temporal data has been ingested as numerical, so conversion to date/time must be applied
		else if (datasetId.getDatatype() == DataType.Type.DATE_TIME)
			val = myAssistant.formatDateValue(datasets.get(datasetId.getHashKey()).get(oid));
		else   // Non-transformed dataset
			val = myAssistant.formatAttrValue(datasets.get(datasetId.getHashKey()).get(oid));
		
		// Handle NULL in attribute values
		attr.setValue(((val != null) ? ((val.getClass().isArray()) ? Arrays.toString((String[]) val) : String.valueOf(val)) : ""));
		
		return attr;
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
	
	
	/**
	 * Provides the similarity scores between a query point (on a specific attribute) and a sample collection.
	 * @param task  Identifier (hash key) of the attribute dataset, in order to apply the relevant distance metric.
	 * @param qPoint  A multi-dimensional (embedded) query point created from the original value on a single attribute.
	 * @param sample  A sample collection of (embedded) points randomly extracted from the original dataset.
	 * @return  A list of top-k similarity scores from the sample w.r.t. query point.
	 */
	private List<Double> findScoresFromSample(String task, Point qPoint, List<Point> sample) {   //, int k
		
		List<Double> scores = new ArrayList<Double>();	
		for (Point p: sample) {
//			double d = metricSimilarities.get(task).calc(qPoint, p);
//			System.out.println(qPoint.toString() + " <-> " + p.toString() + " : " + d);
			scores.add(metricSimilarities.get(task).calc(qPoint, p));
		}
/*		
		// Keep only the top-k similarity scores
		scores.sort(Comparator.reverseOrder());
		return scores.subList(0, k);   // k  is the number of highest similarity scores to keep.
*/		
		return scores;
	}
	
}
