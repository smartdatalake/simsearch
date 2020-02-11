package eu.smartdatalake.simsearch;

import java.io.FileReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import eu.smartdatalake.simsearch.numerical.BPlusTree;
import eu.smartdatalake.simsearch.numerical.ExponentialSimilarity;
import eu.smartdatalake.simsearch.numerical.INormal;
import eu.smartdatalake.simsearch.numerical.DataReader;
import eu.smartdatalake.simsearch.numerical.ProportionalSimilarity;
import eu.smartdatalake.simsearch.numerical.UnityNormal;
import eu.smartdatalake.simsearch.numerical.ZNormal;
import eu.smartdatalake.simsearch.ranking.NoRandomAccessRanking;
import eu.smartdatalake.simsearch.ranking.RankAggregator;
import eu.smartdatalake.simsearch.ranking.ThresholdRanking;
import eu.smartdatalake.simsearch.categorical.CategoricalSimilarity;
import eu.smartdatalake.simsearch.categorical.TokenSetCollection;
import eu.smartdatalake.simsearch.categorical.TokenSetCollectionReader;


/**
 * Coordinator of several concurrently executed similarity queries. 
 * Ranked top-k results issued progressively.
 * Available ranking methods: Threshold, No Random Access. 
 * A JSON config file contains specifications for each query,
 * and configures the main (top-k) rank aggregation process. 
 * EXECUTION command: 
 * java -jar target/simsearch-0.0.1-SNAPSHOT-jar-with-dependencies.jar config.json
 */

public class Coordinator {
	
	public static void main(String[] args) {
/*		
		Runtime runtime = Runtime.getRuntime();
		long usedMemoryBefore = runtime.totalMemory() - runtime.freeMemory();
		logStream.println("Initial memory footprint:" + (1.0 * usedMemoryBefore)/(1024*1024) + "MB");
*/		
		// List of queues that collect results from each running task
		List<ConcurrentLinkedQueue<Result>> queues = new ArrayList<ConcurrentLinkedQueue<Result>>();
		List<Thread> tasks = new ArrayList<Thread>();
		List<ISimilarity> similarities = new ArrayList<ISimilarity>();
		List<INormal> normalizations = new ArrayList<INormal>();
		List<HashMap<?,?>> datasets = new ArrayList<HashMap<?,?>>();
		List<Double> weights = new ArrayList<Double>();
		
		// List of atomic booleans to control execution of the various threads
		List<AtomicBoolean> runControl = new ArrayList<AtomicBoolean>();
		
		long duration;
		
		try {
			RankAggregator aggregator = null;
			String rankingMethod = "threshold";   //Default ranking algorithm
			PrintStream outStream = null;
			String outColumnDelimiter =";";   //Default delimiter for values in the output file
			boolean outHeader = true;
			PrintStream logStream = null;
			
			String configFile = args.length > 0 ? args[0] : "config.json";

			/* READ PARAMETERS */
			JSONParser jsonParser = new JSONParser();
			JSONObject config = (JSONObject) jsonParser.parse(new FileReader(configFile));

			int topk = 0;   // top-k value for ranked aggregated results
			
			JSONArray queries = null;
			
			try {
				topk = Integer.parseInt(String.valueOf(config.get("k")));
				
				//Determine the ranking algorithm to be used for top-k aggregation
				rankingMethod = String.valueOf(config.get("algorithm"));
				
				outColumnDelimiter = String.valueOf(config.get("column_delimiter"));
				if (outColumnDelimiter == null || outColumnDelimiter.equals(""))
					outColumnDelimiter = " ";
				outHeader = Boolean.parseBoolean(String.valueOf(config.get("header")));
				
				// output file
				String outputFile = String.valueOf(config.get("output_file"));
				outStream = new PrintStream(outputFile);
				
				// log file
				String logFile = String.valueOf(config.get("log_file"));
				logStream = new PrintStream(logFile);
				
				// Array of specified queries
				queries = (JSONArray) config.get("queries");
				
			} catch(Exception e) {
				e.printStackTrace();
			}

			if (queries != null) {
		        Iterator it = queries.iterator();
		        while (it.hasNext()) {

		        	JSONObject queryConfig = (JSONObject) it.next();

					// operation
			        String name = String.valueOf(queryConfig.get("name"));
					String operation = String.valueOf(queryConfig.get("operation"));
	
					// file parsing
					String columnDelimiter = String.valueOf(queryConfig.get("column_delimiter"));
					if (columnDelimiter == null || columnDelimiter.equals(""))
						columnDelimiter = " ";
					boolean header = Boolean.parseBoolean(String.valueOf(queryConfig.get("header")));
					
					//Similarity measure to be used
					String simFunction = String.valueOf(queryConfig.get("similarity"));
					ISimilarity<?> simMeasure = null;
					
					// Queue that will be used to collect query results
					ConcurrentLinkedQueue<Result> resultsQueue = new ConcurrentLinkedQueue<Result>();
					
					// settings for top-k similarity search on categorical values
					if (operation.equalsIgnoreCase("categorical_topk")) {
	
						logStream.println("***********THREAD #" + (tasks.size() + 1) + " " + name + " (" + operation
								+ ") ***********");
						// input dataset
						String inputFile = String.valueOf(queryConfig.get("input_file"));
						int maxLines = Integer.parseInt(String.valueOf(queryConfig.get("max_lines")));
						
						// file parsing
						int colSets = Integer.parseInt(String.valueOf(queryConfig.get("set_column"))) - 1;
						int colTokens = Integer.parseInt(String.valueOf(queryConfig.get("tokens_column"))) - 1;
	
						String tokenDelimiter = String.valueOf(queryConfig.get("token_delimiter"));
						if (tokenDelimiter == null || tokenDelimiter.equals(""))
							tokenDelimiter = " ";
	
						//FIXME: What is the usage of qgram?
	//					int qgram = Integer.parseInt(String.valueOf(queryConfig.get("qgram")));
	//					String tokenizer = String.valueOf(queryConfig.get("tokenizer"));
	
						// QUERY SPECIFICATION
						String searchKeywords = String.valueOf(queryConfig.get("search_keywords"));
						
						//WEIGHT
						double weight = Double.parseDouble(String.valueOf(queryConfig.get("weight")));	
						weights.add(weight);
						
						// Inflate top-k value in order to fetch enough candidates from categorical search
						int collectionSize = 1000 * topk; // FIXME: Avoid hard-coded value 

						TokenSetCollectionReader reader = new TokenSetCollectionReader();
						TokenSetCollection queryCollection = null, targetCollection;
						
						duration = System.nanoTime();
						targetCollection = reader.importFromFile(inputFile, colSets, colTokens, columnDelimiter, tokenDelimiter,
								maxLines, header, logStream);
						
						datasets.add(targetCollection.sets);
						
						//This will create a collection for the query only
						queryCollection = reader.createFromQueryKeywords(searchKeywords, tokenDelimiter, logStream);						
					
						duration = System.nanoTime() - duration;
						logStream.println("Read time: " + duration / 1000000000.0 + " sec.");

						// FIXME: Currently, only Jaccard similarity measure is supported
						if (simFunction.equalsIgnoreCase("jaccard"))
							simMeasure = new CategoricalSimilarity(queryCollection.sets.get("querySet"));
						similarities.add(simMeasure);
						
						//CAUTION! No normalization applied to categorical search queries
						normalizations.add(null);
						
						//Create an instance of the categorical search query (TYPE_CATEGORICAL_TOPK = 0)
						SimSearch catSearch = new SimSearch(0, name, targetCollection, queryCollection, collectionSize, simMeasure, resultsQueue, logStream);
						Thread threadCatSearch = new Thread(catSearch);
						threadCatSearch.setName(name); //"CategoricalSearch" + (tasks.size() + 1));
						tasks.add(threadCatSearch);
						queues.add(resultsQueue);
						runControl.add(catSearch.running);
						
					}
					// settings for top-k similarity search on numerical values
					else if (operation.equalsIgnoreCase("numerical_topk")) {
	
						logStream.println("***********THREAD #" + (tasks.size() + 1) + " " + name + " (" + operation
								+ ") ***********");
						// input dataset
						String inputFile = String.valueOf(queryConfig.get("input_file"));
						int maxLines = Integer.parseInt(String.valueOf(queryConfig.get("max_lines")));
						
						// CAUTION! Numeric values in the data are used as KEYS in the B+-tree index
						//...and identifiers in the data are considered as VALUES in the index
						int colKey = Integer.parseInt(String.valueOf(queryConfig.get("value_column"))) - 1;
						int colValue = Integer.parseInt(String.valueOf(queryConfig.get("key_column"))) - 1;
					
						// QUERY SPECIFICATION
						Double searchingKey = Double.parseDouble(String.valueOf(queryConfig.get("search_value")));

						// Determines whether numeric values should be normalized
						String normalized = String.valueOf(queryConfig.get("normalized"));
						
						// INDEX BUILDING
						duration = System.nanoTime();
						
						// Consume specific columns from input file and ...
						// ...build B+-tree on the chosen (key,value) pairs
						DataReader dataReader = new DataReader();
						INormal normal = null;
						
						HashMap<String, Double> targetData = dataReader.read(inputFile, maxLines, colKey, colValue, columnDelimiter, header, logStream);
						datasets.add(targetData);
						
						BPlusTree<Double, String> index = null;
						// Apply normalization (if specified) against input dataset
						if (normalized.equalsIgnoreCase("z")) {
							normal = new ZNormal(dataReader.avgVal, dataReader.stDev);
							index = dataReader.buildNormalizedIndex(targetData, normal, logStream);
							// CAUTION! The search key must be also normalized!
							searchingKey = normal.normalize(searchingKey);   // dataReader.normalizeZscore
							logStream.println("Normalized search key:" + searchingKey);
						}
						else if (normalized.equalsIgnoreCase("unity")) {
							normal = new UnityNormal(dataReader.avgVal, dataReader.minVal, dataReader.maxVal);
							index = dataReader.buildNormalizedIndex(targetData, normal, logStream);
							// CAUTION! The search key must be also normalized!
							searchingKey = normal.normalize(searchingKey); //dataReader.normalizeMinMax(searchingKey);  
							logStream.println("Normalized search key:" + searchingKey);
						}
						else
							index = dataReader.buildIndex(targetData, logStream);
	
						//Remember the kind of normalization applied against this dataset
						normalizations.add(normal);
							
						duration = System.nanoTime() - duration;
						logStream.println("Index created in " + duration / 1000000000.0 + " sec.");
						logStream.println(
								"Index contains " + index.numNodes + " internal nodes and " + index.numLeaves + " leaf nodes.");				
	
						//WEIGHT
						double weight = Double.parseDouble(String.valueOf(queryConfig.get("weight")));	
						weights.add(weight);
						
						// Determine the range of keys needed as denominator in similarity calculations
						double domainKeyRange = (searchingKey.compareTo(index.calcMinKey()) < 0)
								? (index.calcMaxKey() - searchingKey)
								: ((searchingKey.compareTo(index.calcMaxKey()) > 0)
										? (searchingKey - index.calcMinKey())
										: (index.calcMaxKey() - index.calcMinKey()));
	
						// Determine the similarity measure to be used
						if (simFunction.equals("proportional"))
							simMeasure = new ProportionalSimilarity(searchingKey, domainKeyRange);
						else {   // By default, use exponential decay similarity scores
							double decay = Double.parseDouble(String.valueOf(queryConfig.get("decay")));
							simMeasure = new ExponentialSimilarity(searchingKey, domainKeyRange, decay);
						}
						similarities.add(simMeasure);
						
						//Create an instance of the numerical search query (TYPE_NUMERICAL_TOPK = 2)
						// collectionSize = -1 -> no prefixed bound on the number of results to fetch from numerical similarity search
						SimSearch numSearch = new SimSearch(2, name, index, searchingKey, -1, simMeasure, resultsQueue, logStream);
						Thread threadNumSearch = new Thread(numSearch);
						threadNumSearch.setName(name); //"NumericalSearch" + (tasks.size() + 1));
						tasks.add(threadNumSearch);
						queues.add(resultsQueue);
						runControl.add(numSearch.running);					
					}
					//TODO: Include other operations...
					//else if (operation.equalsIgnoreCase("spatial_knn")) {
	
					else {
						logStream.println("Unknown operation specified: " + operation);
					}
				}	
			}
			
			logStream.println("****************RANKED AGGREGATION PROCESSING*********************");
			
			// Instantiate the ranked aggregator that will handle results from the various threads
			switch(rankingMethod){
			case "no_random_access":
				aggregator = new NoRandomAccessRanking(tasks, queues, runControl, topk, outStream, outColumnDelimiter, outHeader, logStream);
				break;
			case "threshold":
				aggregator = new ThresholdRanking(datasets, similarities, weights, normalizations, tasks, queues, runControl, topk, outStream, outColumnDelimiter, outHeader, logStream);
				break;
			default:
				logStream.println("No ranking method specified!");
				break;
			}
			
			// Start all tasks; each query will now start fetching results
			for (Thread task : tasks) {
				task.start();
			}
/*
			long usedMemoryAfter = runtime.totalMemory() - runtime.freeMemory();
		    logStream.println("Memory footprint: " + (1.0 * (usedMemoryAfter - usedMemoryBefore))/(1024*1024) + "MB");
*/		    
			//Perform the ranked aggregation process
			duration = System.nanoTime();
			
			aggregator.proc();
			
			duration = System.nanoTime() - duration;
			logStream.println("Ranked aggregation processing time: " + duration / 1000000000.0 + " sec.");
			System.out.println("Process conculuded successfully.");
			System.exit(0);


		} catch (Exception e) { // InterruptedException
			e.printStackTrace();
			System.out.println("Process terminated abnormally.");
			System.exit(1);
		}
	}
}