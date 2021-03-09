package eu.smartdatalake.simsearch.pivoting;

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

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

import eu.smartdatalake.simsearch.request.SearchRequest;
import eu.smartdatalake.simsearch.request.SearchSpecs;
import eu.smartdatalake.simsearch.Assistant;
import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.csv.lookup.Word2VectorTransformer;
import eu.smartdatalake.simsearch.engine.IResult;
import eu.smartdatalake.simsearch.engine.OutputWriter;
import eu.smartdatalake.simsearch.engine.QueryValueParser;
import eu.smartdatalake.simsearch.engine.SearchResponse;
import eu.smartdatalake.simsearch.engine.SearchResponseFormat;
import eu.smartdatalake.simsearch.manager.DatasetIdentifier;
import eu.smartdatalake.simsearch.manager.TransformedDatasetIdentifier;
import eu.smartdatalake.simsearch.pivoting.rtree.Entry;
import eu.smartdatalake.simsearch.pivoting.rtree.MultiMetricSimilaritySearch;
import eu.smartdatalake.simsearch.pivoting.rtree.NearestEntry;
import eu.smartdatalake.simsearch.pivoting.rtree.RTree;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Point;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Rectangle;
import eu.smartdatalake.simsearch.ranking.RankedResult;
import eu.smartdatalake.simsearch.ranking.ResultFacet;

/**
 * Creates a pivot-based multi-dimensional RR*-tree and then handles multi-attribute similarity search requests. 
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
	
	// Fixed scaling factors determined during tree construction; alternatively, they can be computed dynamically at query time
	ScaleFactors scaleFactors;
	
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
	    		sample.add(randomSample(nonNullSubset, Constants.NUM_SAMPLES));
	/*	    		
		    		for (Point p: randomSet) {
		    			System.out.println(p.toString());
		    		}
		    		System.out.println("*************Chosen " + randomSet.size() + " objects for " + m + "-th distance.**********************");
	*/	    		
	    	}
	    	
	    	// This process does NOT choose the actual pivots, but only a suitable number of pivots per distance
	    	PivotSetting setting = new PivotSetting(M, R, ref.metrics, sample, log);
	    	int[] pivotsPerMetric = setting.greedyMaximization();
	    	
	    	// FIXME: Assign the average NN distances per distance as the respective scale factors
	    	scaleFactors.scale = setting.getEpsilonThresholds();
	    	
	    	// Select number of pivots per attribute (distance)
	    	int cntPivots = 0;
	    	for (String attr: records.keySet()) {
	    		int m = ref.getAttributeOrder(attr);   // Metric reference corresponding to this attribute
	    		ref.setStartReference(m, cntPivots);
	    		cntPivots += pivotsPerMetric[m];
	    		ref.setEndReference(m, cntPivots - 1);
	    	}		

	    	// For each attribute (distance) involved in the index (and the queries), identify its respective data source
	    	attrIdentifiers = new String[ref.countMetrics()];
			for (Map.Entry<String, DatasetIdentifier> id: pivotDataIdentifiers.entrySet()) {
				// Attribute name
				String attr = id.getValue().getValueAttribute();
				// The internal hashkey of each dataset is assigned per distance
				attrIdentifiers[ref.getAttributeOrder(attr)] = id.getValue().getHashKey();
			}
			
			duration = System.nanoTime() - duration;
			log.writeln("RR*-tree pivot setting: " + duration / 1000000000.0 + " sec.");
	    		    	
	    	log.writeln("**************RR*-tree: Pivot selection & embedding****************");
	    	duration = System.nanoTime();
	    	
			// Initialize list for pivot values to be determined per distance
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
	    		
	    		// FIXME: Substitute distance values for NaN ordinates with the scale factor used for this attribute
	    		ref.getMetric(m).setNaNdistance(scaleFactors.scale[m]);
	    		log.writeln("PIVOTS for attribute " + ref.getAttribute(m) + " using " + ref.getMetric(m).getClass().getSimpleName() + " distance:");
	    		
	    		// Pivot selection is only based on the values concerning a particular attribute and distance
	    		PivotSelector selector = new PivotSelector(ref.getMetric(m), new ArrayList<Point>(records.get(attr).values()), log);
	    		// Number of pivots to select for this attribute have been estimated
	    		distances[m] = selector.embed(ref.countDimensionReferenceValues(m));   		
	    		// Retain the selected pivots for subsequently embedding query points
	    		pivots.get(m).addAll(selector.getPivots());
	    	}
	    	
	    	// Construct array of embeddings per input object
	    	// This will become an R-dimensional point to be indexed in the tree
	    	List<Entry<Object, Point>> points = new ArrayList<Entry<Object, Point>>();
	    	Entry<Object, Point> entry = null; 
	    	int c = 0;
	    	double[] val;
	    	// IMPORTANT! Assuming that entity identifiers are identical for all lists of input records
	    	for (String key: records.get(ref.getAttribute(0)).keySet()) {
	    		val = new double[R];
	    		int n = 0;
	    		for (int j = 0; j < M; j++) {   // For each attribute (distance)
	    			// Number of pivots may differ per attribute
	    			for (int l = 0; l < ref.countDimensionReferenceValues(j); l++) {
	    				val[n] = distances[j][c][l];	// Embedded distance from pivot
	    				n++;
	    			}
	    		}
	    		entry = Entry.entry(key, Point.create(val));
	    		points.add(entry);
	//	    	System.out.println(Arrays.toString(val));
	    		c++;
	    	}
	    	
	    	duration = System.nanoTime() - duration;
	    	log.writeln("RR*-tree embedding cost: " + duration / 1000000000.0 + " sec.");
	    		    	
	//		System.out.println("*****************R-tree construction using pivots **********************");
			//Create the R*-tree
			duration = System.nanoTime();
			tree = RTree.dimensions(R).maxChildren(Constants.NODE_FANOUT).star().<Object, Point>create(points);
			
	    	// Tree construction statistics 
	    	duration = System.nanoTime() - duration;
	    	log.writeln("RR*-tree construction time: " + duration / 1000000000.0 + " sec.");
	    	log.writeln("Indexed objects: " + tree.size());
	        log.writeln("RR*-tree dimensions: " + tree.dimensions());
	          
	        // MBR of the tree
	        Optional<Rectangle> opt = tree.mbr();
	        opt.ifPresent(mbr -> log.writeln("RR*-tree MBR: " + mbr.toString()));
	      
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
		
		log.writeln("Attribute " + attrName + " was not included in the query specification. A NULL value has been specified with zero zeights.");
		
		return extraSpecs;
	}
	
	
	/**
	 * Searching stage: Given a user-specified configuration, execute the pivot-based similarity search and issue the ranked top-k results.
	 * This method accepts an instance of SearchRequest class.
	 * @param params   An instance of SearchRequest class with the multi-facet search query specifications.
	 * @return  A JSON-formatted response with the ranked results.
	 */
	public SearchResponse[] search(SearchRequest params) {
		
		SearchResponse[] responses;
		String notification = "";  // Any extra notification for the final response

		// Specifications for writing results into an output CSV file (if applicable)
		OutputWriter outCSVWriter = new OutputWriter(params.output);
		
		// Query specifications
		// NOTE: Query may not specify all indexed attributes
		SearchSpecs[] querySpecs = params.queries;
		List<String> queryAttributes = new ArrayList<String>();
		for (int i = 0 ; i < querySpecs.length; i++) {
			// Check if attribute has specified more than once
			if (queryAttributes.contains(querySpecs[i].column.toString()))
				log.writeln("Attribute " + querySpecs[i].column + " has been specified more than once. Only the last specification was used in query evaluation.");
			queryAttributes.add(querySpecs[i].column.toString());	
		}
		
		// Number of weight combinations
        int weightCombinations = querySpecs[0].weights.length;
		
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
			log.writeln("Search request discarded, because no more than top-" + Constants.K_MAX + " results can be returned per query..");
			response.setNotification("Please specify a positive integer value up to " + Constants.K_MAX + " for k and submit your request again.");
			responses[0] = response;
			return responses;
		}
					
		// Weights: dictionary with attribute names as keys
    	Map<String, Double[]> attrWeights = new HashMap<String, Double[]>();
    	
    	// Scale factors to be used in this search
    	double[] scale = scaleFactors.getAll();   // Default values to apply if not specified in the query configuration
 	
        // CAUTION! Query: a multi-dimensional point must be constructed per attribute
        Map<String, Point> qPoint = new TreeMap<String, Point>();   // Original values per attribute
        Point p;
        
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
			// TODO: Handle cases where an array of attributes is used (e.g., lon/lat coordinates for location)
			datasetId = this.findIdentifier(colValueName);
			if (datasetId == null) {
				notification += " Attribute data on " + colValueName + " has not been specified for pivot-based similarity search.";
				log.writeln("Attribute data on " + colValueName + " has not been specified for pivot-based similarity search.");
				continue;
			}
		
			// Parse query value for this attribute
			qColumns[i] = datasetId.getValueAttribute();	
			String[] ordinates = valParser.parseCoordinates(querySpecs[i].value);
			qValues[i] = String.join(",", ordinates);
        	if (ordinates.length > 0) {
        		if (datasetId.needsTransform()) {
        			// Transform original query value into a vector representation according to the appointed transformer
        			p = Point.create(transformers.get(qColumns[i]).getVector(ordinates));
        		}
        		else {
        			// Handling original numerical values in queried attributes
        			p = Point.create(Arrays.stream(ordinates).mapToDouble(Double::parseDouble).toArray());	
        		}
        	}
        	else {
        		// If value is not specified, a NaN-valued point will be created for this attribute with a suitable number of ordinates
        		p = myAssistant.createNaNPoint(ref.getDimension(qColumns[i]));
        		log.writeln("Created NaN point for query value at attribute " + qColumns[i] + ".");
        	}
        	qPoint.put(qColumns[i], p);
        	
        	// Retain the weights associated with this attribute
        	attrWeights.put(qColumns[i], querySpecs[i].weights);
        	
        	// Specify the distance similarity to be used for this attribute (distance)
        	int m = ref.getAttributeOrder(qColumns[i]);   // The distance (attribute) involved
        	// Apply the user-specified scale factor for distances on this attribute
        	if (querySpecs[i].scale != null)
    			scale[m] = querySpecs[i].scale;
        	// Also keep a decay factor if specified by the user for this attribute
        	MetricSimilarity metricSimilarity = new MetricSimilarity(p, ref.getMetric(m), ((querySpecs[i].decay != null) ? querySpecs[i].decay : Constants.DECAY_FACTOR), scale[m], m);
        	// As key employ the same hashkey used for datasets
        	metricSimilarities.put(datasetId.getHashKey(), metricSimilarity);
		}
 	
		log.writeln("Query: " + Arrays.toString(qValues));
		
		// Check whether values have been specified for all queryable attributes
		if (metricSimilarities.values().contains(null)) {
			responses = new SearchResponse[1];
			SearchResponse response = new SearchResponse();
			log.writeln("Query value is missing in at least one attribute. Please check your query specification." );
			response.setNotification("Query value is missing in at least one attribute. Please check your query specification." + notification);
			responses[0] = response;
			return responses;		
		}
		
        // Embedded query point using the same pivots in order to be used during tree traversal
        Point q = Point.create(embed(qPoint));
        log.writeln("Query embedding: " + Arrays.toString(q.mins()));

        /********************QUERY EVALUATION ******************/
        
        duration = System.nanoTime();  
        
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
//			System.out.println("Top-k similarity search results using pivots:");
			int rank = 1;   // ranking of issued results
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
    	
		// Format response
    	// FIXME: No similarity matrix calculated for these results
		SearchResponseFormat responseFormat = new SearchResponseFormat();
		responses = responseFormat.proc(allResults, attrWeights, datasetIdentifiers, datasets, datasets, null, null, metricSimilarities, topk, this.isCollectQueryStats(), outCSVWriter);

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
//			System.out.print("OLD rank: " + entry.getValue());
			sortedResults[rank-1] = (RankedResult) results[entry.getValue()-1];
			sortedResults[rank-1].setRank(rank);
//			System.out.println(" NEW rank: " + rank);
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
		else   // Non-transformed dataset
			val = datasets.get(datasetId.getHashKey()).get(oid);

		// Handle NULL in attribute values
		attr.setValue(((val != null) ? ((val.getClass().isArray()) ? Arrays.toString((String[]) val) : val.toString()) : ""));
	
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
	
}