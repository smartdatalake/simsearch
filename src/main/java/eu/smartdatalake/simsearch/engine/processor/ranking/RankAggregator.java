package eu.smartdatalake.simsearch.engine.processor.ranking;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import eu.smartdatalake.simsearch.Assistant;
import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.engine.IResult;
import eu.smartdatalake.simsearch.engine.measure.ISimilarity;
import eu.smartdatalake.simsearch.engine.processor.RankedResult;
import eu.smartdatalake.simsearch.engine.processor.ResultFacet;
import eu.smartdatalake.simsearch.engine.weights.Estimator;
import eu.smartdatalake.simsearch.manager.DataType;
import eu.smartdatalake.simsearch.manager.DatasetIdentifier;
import eu.smartdatalake.simsearch.manager.ingested.numerical.INormal;


/**
 * General framework and auxiliary methods for rank aggregation.
 * @param <K>  Type variable to represent the entity identifiers.
 * @param <V>  Type variable to represent the attribute values (numbers, spatial locations, or sets of tokens).
 */
public class RankAggregator<K, V> implements IRankAggregator {

	Logger log = null;
	Assistant myAssistant;
	
	AggregateResult aggResult;
	double score;
	int numTasks;
	int topk;       // Number of ranked aggregated results to collect
 
	// Array of collections with ranked aggregated results at current iteration; one collection per weight combination
	AggregateResultCollection[] curResults;

	// Collection of queues that collect results from each running task
	Map<String, RankedList> queues;
	Map<String, Thread> tasks;
	
	// Collection of all data/index datasetIdentifiers involved in the search
	Map<String, DatasetIdentifier> datasetIdentifiers;
	
	// Weights
	Map<String, Double[]> weights;
	int weightCombinations;
	
	// Normalization functions
	Map<String, INormal> normalizations;
	
	// Look-up tables built for the entire attribute datasets as needed in random access similarity calculations
	// Using the dataset hash key as a reference to the collected values for each attribute
	Map<String, Map<K, V>> lookups;
	
	// Collection of atomic booleans to control execution of the various threads
	Map<String, AtomicBoolean> runControl = new HashMap<String, AtomicBoolean>();

	// Collection of similarity functions to be used in random access calculations
	Map<String, ISimilarity> similarities;
	
	// Collection of the ranked results to be given as output per weight combination
	ResultCollection[] results;
	
	// Sum of weights per combination
	double[] sumWeights; 
	
	// Estimator to auto-configure weights for attribute(s) in case of no user-specified values 
	Estimator estimator;
	boolean missingWeights = false;   // By default, assume that all weights are specified 
	

	/**
	 * Constructor
	 * @param datasetIdentifiers List of the attributes involved in similarity search queries.
	 * @param lookups   Dictionary of the various data collections involved in the similarity search queries.
	 * @param similarities   Dictionary of the similarity measures applied in each search query.
	 * @param weights  Dictionary of the (possibly multiple alternative) weights per attribute to be applied in scoring the final results. 
	 * @param normalizations  Dictionary of normalization functions to be applied in data values during random access.
	 * @param tasks  Collection of running threads; each one executes a query and it is associated with its respective queue that collects its results.
	 * @param queues  Collection of the ranked lists collecting results from each search query.
	 * @param runControl  Collection of boolean values indicating the status of each thread.
	 * @param topk  The count of ranked aggregated results to collect, i.e., those with the top-k (highest) aggregated similarity scores.
	 * @param log  Handle to the log file for notifications and execution statistics.
	 */
	public RankAggregator(Map<String, DatasetIdentifier> datasetIdentifiers, Map<String, Map<K, V>> lookups, Map<String, ISimilarity> similarities, Map<String, Double[]> weights, Map<String, INormal> normalizations, Map<String, Thread> tasks, Map<String, RankedList> queues, Map<String, AtomicBoolean> runControl, int topk, Logger log) {
		
		myAssistant = new Assistant();
		this.log = log;
		this.datasetIdentifiers = datasetIdentifiers;
		this.lookups = lookups; 
		this.similarities = similarities;
		this.tasks = tasks;
		this.queues = queues;
		this.weights = weights;
		this.normalizations = normalizations;
		this.runControl = runControl;
		this.topk = topk;
		
		// Initialize number of combinations of weights to apply
		weightCombinations = 1;
		
		// In case of non-specified weights, create estimators so that they can be assigned dynamically later
		estimator = new Estimator();
		for (String task : this.weights.keySet()) {
			// Weights will be automatically determined from statistical analysis of the partial results per queue
			if (weights.get(task) == null) {
				estimator.setMissingWeight(task);
				missingWeights = true;
			}
			else
				if (weights.get(task).length > weightCombinations)
					weightCombinations = weights.get(task).length;
		}
		
		// Array of aggregated results collected at the current iteration for each combination of weights
		curResults = new AggregateResultCollection[weightCombinations];

		// Array of collection of results; one collection (list) per combination of weights
		results = new ResultCollection[weightCombinations];
		
		// Initialize array structures
		for (int w = 0; w < weightCombinations; w++) {
			results[w] = new ResultCollection();
		}
	}

	
	
	/**
	 * Fetches all partial results (candidates) and populates priority queues.
	 * In case of missing weights, it also estimates suitable weights per attributes based on the scores of collected candidates.
	 */
	protected void fetch() {
		
		boolean fetching = true;
		boolean allScalesSet = false;
		PartialResult<K, V> pRes = null;
		
		try {
			// Wait until all similarity search queries are concluded ...
			// ...and the results in their respective priority queues are finalized
			while (fetching) {
				fetching = false;
				// Wait until all scale factors for similarity scores have been set in all partial results from each task
				// Otherwise, scale factors may have been set arbitrarily.
				while (allScalesSet == false) {
					allScalesSet = true;
					TimeUnit.NANOSECONDS.sleep(100);
					for (String task : tasks.keySet()) {
						// In case that the task is completed and no scale has been set, this means that all results are exact and no scale is needed
						allScalesSet = allScalesSet && (!runControl.get(task).get() || this.similarities.get(task).isScaleSet());
					}
				}
				// Wait until all candidates are fetched
				for (String task : tasks.keySet()) {
					if (tasks.get(task).isAlive())
						fetching = true;
				}
			}
			
			// Populate look-ups with the contents of the priority queues
			for (String task : tasks.keySet()) {
//				System.out.println("QUEUE size for " + datasetIdentifiers.get(task).getValueAttribute() + ": " + queues.get(task).size());
		        Iterator<PartialResult> qIter = queues.get(task).iterator(); 
		        List<Double> scores = new ArrayList<Double>();
		        while (qIter.hasNext()) { 
		        	pRes = qIter.next();
		        	scores.add(pRes.getScore());  
		        }
		        
		        // Specify input values (scores) to be used in estimating weights for this attribute      
		        if (missingWeights) {
		        	estimator.setInput(task, scores);
		        }	
			}
			
			// Invoke weight estimation if any is missing for specific attributes
			if (missingWeights) {
				//estimator.proc();		// Estimate based on standard deviation of scores
				estimator.proc(topk);	// ALTERNATIVE: Estimate derived from percentiles and depends on the top-k parameter
				weights.putAll(estimator.getWeights(weightCombinations));
				// Log estimated weights, if any
				String msg = "Weights assigned: ";
				for (String task : estimator.getAttributesWithoutWeight())
					msg += datasetIdentifiers.get(task).getValueAttribute() + " -> " +  estimator.getWeight(task) + "; ";
				log.writeln(msg);	
			}
			
			// Calculate the sum of weights for each combination
			sumWeights = new double[weightCombinations];
			Arrays.fill(sumWeights, 0.0);
			for (int w = 0; w < weightCombinations; w++) {
				for (String taskKey : tasks.keySet()) {
					sumWeights[w] += this.weights.get(taskKey)[w];
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	/**
	 * Inserts the i-th ranked result to the output list. Rank is based on the overall score; ties in scores are resolved arbitrarily.
	 * @param i  The rank to the assigned to the output result.
	 * @param w  The identifier of the weight combination to be applied on the scores.
	 * @param item   The original identifier of this item in the dataset
	 * @param score  The overall (weighted) score of this result.
	 * @param exact  Boolean indicating whether the ranking of this result is exact or not.
	 * @return  A Boolean value: True, if extra result(s) have been issued; otherwise, False.
	 */
	protected boolean issueRankedResult(int i, int w, String item, double score, boolean exact) {

		// Skip any already issued entity when reporting extra (approximately scored) results for this weight combination
		if (results[w].contains(item))
			return true;
		
		i++;  // Showing rank as 1,2,3,... instead of 0,1,2,...
		// Stop once topk results have been issued, even though there might be ties having the same score as the topk result
		if (i > topk)
			return false;
		
		// Create a new resulting item and report its rank and its original identifier
		// Includes values and scores per individual attribute; this is also needed for the similarity matrix
		RankedResult res = new RankedResult(tasks.size());
		res.setId(item);
		res.setRank(i);
		
		// ... also its original values at the searched attributes and the calculated similarity scores
		int j = 0;
		for (String task : tasks.keySet()) {
			ResultFacet attr = new ResultFacet();
			attr.setName(this.datasetIdentifiers.get(task).getValueAttribute());
			if (this.lookups.get(task).get(item) == null) {   	 // By default, assign zero similarity for NULL values in this attribute							
				attr.setValue("");   // Use blank string instead of NULL
				attr.setScore(0.0);
			}
			else {	  // Not NULL attribute value
				// Temporal data has been ingested as numerical, so conversion to date/time must be applied
				if (this.datasetIdentifiers.get(task).getDatatype() == DataType.Type.DATE_TIME) //&& (!this.datasetIdentifiers.get(task).getDataSource().isInSitu()))
					attr.setValue(myAssistant.formatDateValue(this.lookups.get(task).get(item)));  
				else  // Any other values
					attr.setValue(myAssistant.formatAttrValue(this.lookups.get(task).get(item)));
				// Estimate its individual similarity score
				if (this.normalizations.get(task) != null)   // Apply normalization, if specified
					attr.setScore(this.similarities.get(task).calc(this.normalizations.get(task).normalize(this.lookups.get(task).get(item))));
				else
					attr.setScore(this.similarities.get(task).calc(this.lookups.get(task).get(item)));
			}
			res.getAttributes()[j] = attr;
			j++;
		}
		//... and its overall aggregated score
		res.setScore(score / sumWeights[w]);   // Weighted aggregate score over all running tasks (one per queried attribute)
	
		// Indicate whether this ranking should be considered exact or not
		res.setExact(exact);
		
		// Issue result to the output queue
		results[w].add(res);
//		log.writeln(i + " RESULT: " + res.getId() + " " + res.getScore());

		return true;
	}


	@Override
	public IResult[][] proc() {
		return null;
	}

}
