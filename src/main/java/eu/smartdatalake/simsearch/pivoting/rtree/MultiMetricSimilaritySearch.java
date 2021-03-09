package eu.smartdatalake.simsearch.pivoting.rtree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.pivoting.MetricReferences;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Geometry;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Point;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Rectangle;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.internal.GeometryUtil;
import eu.smartdatalake.simsearch.pivoting.rtree.internal.Comparators;
import eu.smartdatalake.simsearch.pivoting.rtree.internal.NearestEntryDefault;
import eu.smartdatalake.simsearch.pivoting.rtree.internal.util.CandidatePriorityQueue;

/**
 * Implements the method for top-k similarity search against the an index holding (embedded) multi-dimensional points using various distances.
 * Also offers an auxiliary method for estimating scale factors on distances per attribute (distance). 
 */
public class MultiMetricSimilaritySearch {

	Logger log = null;
	
	// Input datasets and their identifiers used for computing exact distances
	Map<String, Map<?,?>> datasets;
	String[] attrIdentifiers;
	
	MetricReferences refs;	// Reference values (pivots) and distance metrics used per attribute
	double[] W;  			// Vector of M weights: one weight per distance metric (attribute)
	double[] scale;			// Scale factor to be applied in computed distances; one such factor per distance metric (attribute)
	int M;   				// Total number of distance metrics (i.e., queryable attributes)
	
	double sumWeights; 		// Sum of the M weights
	
	/**
	 * Constructor
	 * @param datasets  Dictionary of input datasets used in creating the underlying index.
	 * @param attrIdentifiers  Attribute identifiers per distance metric, each corresponding to the input datasets.
	 * @param r  Representation of distances and number of reference (pivot) values per attribute.
	 * @param w	 Array of weights per attribute to be applied in estimating overall distances.
	 * @param s  Array of scale factors per attribute to be applied in estimating overall distances.
	 * @param log  Handle to the log file for notifications and execution statistics.
	 */
	public MultiMetricSimilaritySearch(Map<String, Map<?,?>> datasets, String[] attrIdentifiers, MetricReferences r, double[] w, double[] s, Logger log) {

		this.log = log;
		this.datasets = datasets;
		this.attrIdentifiers = attrIdentifiers;
		this.W = w;
		this.scale = s;
		this.refs = r;
		this.M = r.countMetrics();		
		
		// Calculate the sum of all weights to be used as denominator when weighing distances across all attributes
		this.sumWeights = Arrays.stream(w).sum();
	}
	
	
	/**
	 * Recursively examining whether to continue searching the subtree of a node; tree traversal should start from the root.
	 * @param node  Node of the tree currently searched during tree traversal.
	 * @param q  The multi-dimensional representation of the query point; Actually, the MBR of the query location is used as focus in the search.
	 * @param qOrig  The original query values per attribute.
	 * @param k  The number k of entities to fetch as most similar to the query.
	 * @return  A collection of the k entities held in the tree that are most similar to the query.
	 */
    public <T, S extends Geometry, D> Iterable<NearestEntry<T, S, Double>> search(Node<T, S> node, Point q, Map<String, Point> qOrig, int k) {
     	
    	// Collector of qualifying results
    	List<NearestEntry<T, S, Double>> results = new ArrayList<NearestEntry<T, S, Double>>();
    	
    	// Priority queue of visited nodes and entries sorted by ascending distance from query q
    	// Distance calculations are weighted and may involve a different distance per attribute
    	CandidatePriorityQueue<Candidate<Object, Double>> Q = new CandidatePriorityQueue<Candidate<Object, Double>>(Comparators.ascendingDistance());
    	
    	Candidate<Object, Double> element;
    	Node<?, ?> n;
    	
    	// Add root node to the queue with its distance from q
    	Q.enqueue(new Candidate<Object, Double>(node, minDistMBR(q, node.geometry().mbr())));
 	
    	int cnt = 0;
    	double dist = 0.0;
    	// Iterate over elements in the priority queue until it gets exhausted or the top-k results have been collected
    	while (!Q.isEmpty()) {
    		// Get the next element from the priority queue: it can be either a node (leaf or internal) or an entry (i.e., a multi-dimensional object)
    		element = Q.dequeue();   		
    		// Handle types of nodes and report results
    		if (element.node() instanceof Entry<?,?>) {  // This is an entry (i.e., a multi-dimensional object)
    			Entry<?, ?> e = (Entry<?, ?>) element.node();    
    			// IMPORTANT! Compute exact distance between the original query values and the respective (NOT embedded) values of this element
    			// Distance for candidate results should be calculated between objects, not based on their MBRs (i.e., embeddings)
    			dist = exactDistance(e.value(), qOrig);
    			if ((!Q.isEmpty()) && (dist > Q.peek().distance())) {
    				// Exact distance is less than the one (lower bound) held in the head of the priority queue; object should be inserted back to the queue
    				Q.enqueue(new Candidate<Object, Double>(e, dist));
    			}
    			else {    			
    				// Report next result with the exact distance
    				results.add(new NearestEntryDefault<T, S, Double>((T)e.value(), (S) e.geometry(), dist));
    				cnt++;
    			}	
    		}
    		else {   // This is a node, either internal or leaf			
    			n = (Node<?, ?>) element.node();
	    		if (n.isLeaf()) {  	// Leaf node
	    			// Push all its entries into the queue according to their distances from the query point
	    			for (Entry<?, ?> e : ((Leaf<?, ?>) n).entries()) {
	    				Q.enqueue(new Candidate<Object, Double>(e, minDistPoint(q.mins(), e.geometry().mbr().mins())));
	    			}
	    		}
	    		else {   			// Internal node
	    			// Push all its children into the queue according to their distances from the query point
	    			for (Node<?, ?> c : ((NonLeaf<?, ?>) n).children()) {
	    				Q.enqueue(new Candidate<Object, Double>(c, minDistMBR(q, c.geometry().mbr())));
	    			}
	    		}
    		}
    		
    		// Stop searching once k-NN entries have been collected
    		if (cnt >= k)
    			break;
    	}
/*    	
    	if (log != null)
    		log.writeln("Max number of elements ever held in the priority queue:" + Q.getMaxElements());
*/    	      
        return results;
    }
 
    
    /**
     * Auxiliary method that estimates a factor to be used for scaling distances on a particular attribute (distance).
     * Involves a k-NN query that involves only values regarding a single attribute (distance); weights for the rest are set to 0.
     * @param node  Node of the tree currently searched during tree traversal.
	 * @param q  The multi-dimensional representation of the query point; Actually, the MBR of the query location is used as focus in the search.
	 * @param k  The number k of entities to keep as most similar to query q (on a single attribute only).
     * @return  The distance of the k-th most similar entity from query q; if this distance is 0, then the first non-zero distance is returned.
     */
    public <T, S extends Geometry, D> double getScaleFactor(Node<T, S> node, Point q, int k) {
 
    	double dist = 0.0;   // Initialize distance
    	
    	// Collector of qualifying results
    	List<NearestEntry<T, S, Double>> results = new ArrayList<NearestEntry<T, S, Double>>();
    	
    	// Priority queue of visited nodes and entries sorted by ascending distance from query q
    	// Distance calculations are weighted and may involve a different distance per attribute
    	CandidatePriorityQueue<Candidate<Object, Double>> Q = new CandidatePriorityQueue<Candidate<Object, Double>>(Comparators.ascendingDistance());
    	
    	Candidate<Object, Double> element;
    	Node<?, ?> n;
    	
    	// Add root node to the queue with its distance from q
    	Q.enqueue(new Candidate<Object, Double>(node, minDistMBR(q, node.geometry().mbr())));
 	
    	int cnt = 0;
    	while (!Q.isEmpty()) {
    		// Get the next element from the priority queue: it can be either a node (leaf or internal) or an entry (i.e., a multi-dimensional object)
    		element = Q.dequeue();   		
    		// Handle types of nodes and report results
    		if (element.node() instanceof Entry<?,?>) {  // This is an entry (i.e., a multi-dimensional object)
    			Entry<?, ?> e = (Entry<?, ?>) element.node();      
    			if ((!Q.isEmpty()) && (element.distance() > Q.peek().distance())) {
    				// Actually, the distance must be calculated between objects, not based on their MBRs
    				// Since objects are always multi-dimensional points, these are equivalent
    				Q.enqueue(new Candidate<Object, Double>(e, element.distance()));
    			}
    			else {
    				// Report next result
    				results.add(new NearestEntryDefault<T, S, Double>((T)e.value(), (S) e.geometry(), element.distance()));
    				cnt++;
    			}	
    		}
    		else {   // This is a node, either internal or leaf			
    			n = (Node<?, ?>) element.node();
	    		if (n.isLeaf()) {  	// Leaf node
	    			// Push all its entries into the queue according to their distances from the query point
	    			for (Entry<?, ?> e : ((Leaf<?, ?>) n).entries()) {
	    				Q.enqueue(new Candidate<Object, Double>(e, minDistPoint(q.mins(), e.geometry().mbr().mins())));
	    			}
	    		}
	    		else {   			// Internal node
	    			// Push all its children into the queue according to their distances from the query point
	    			for (Node<?, ?> c : ((NonLeaf<?, ?>) n).children()) {
	    				Q.enqueue(new Candidate<Object, Double>(c, minDistMBR(q, c.geometry().mbr())));
	    			}
	    		}
    		}
    				
    		// Stop searching once at least k entries have been collected and the k-th distance is greater than zero
    		if (cnt >= k) {
    			dist = results.get(results.size()-1).distance();
    			if (dist > 0.0)
    				break;
    		}
    	}
/*    	
    	if (log != null)
    		log.writeln("Max number of elements ever held in the priority queue: " + Q.getMaxElements());
*/    	      
        return dist;
    }
    
    
    /**
     * Weighted distance bound between a multi-dimensional query point from a multi-dimensional rectangle.
     * @param q  M-dimensional query point (already embedded according to pivots).
     * @param r  R-dimensional MBR indexed in the tree structure.
     * @return  Weighted distance bound involved in similarity search.
     */
    private double minDistMBR(Point q, Rectangle r) {
    	
    	return  minDistMBR(q.mins(), r);
    }
   
    
    /**
     * Weighted distance bound between a multi-dimensional query point from a multi-dimensional rectangle.
     * @param q  Coordinate vector of an M-dimensional query point (already embedded according to pivots).
     * @param r  R-dimensional MBR indexed in the tree structure.
     * @return  Weighted distance bound involved in similarity search.
     */
	private double minDistMBR(double q[], Rectangle r) {
		
		double minDist = 0.0;
		double maxMetric;
		double distance;
		
		// Iterate over all reference values
    	for (int m = 0; m < M; m++) {
    		maxMetric = 0.0;
    		// Take the max distance of q from all MBRs per distance metric
    		for (int i = refs.getStartReference(m); i <= refs.getEndReference(m); i++) {
    			distance = 0.0;
    			if (q[i] < r.min(i))
    				distance = r.min(i) - q[i];
    			if (q[i] > r.max(i))
    				distance = q[i] - r.max(i);
    			// max distance found among all reference points for this distance
    			maxMetric = GeometryUtil.max(maxMetric, distance);   
    		}
    		minDist += W[m] * rescale(maxMetric, m);   // scaled distance
    	}

		return (minDist / this.sumWeights);	
	}
	
	
    /**
     * Weighted distance bound between a multi-dimensional query point from a multi-dimensional reference object.
     * @param q  M-dimensional query point (already embedded according to pivots).
     * @param v  R-dimensional embedded object (R may be larger than M).
     * @return  Weighted distance bound involved in similarity search.
     */
	private double minDistPoint(Point q, Point v) {
		
		return minDistPoint(q.mins(), v.mins());
	}
	
	
    /**
     * Weighted distance bound between a multi-dimensional query point from a multi-dimensional embedded object.
     * @param q  Coordinate vector of an R-dimensional query point (already embedded according to pivots).
     * @param v  R-dimensional vector holding the embedding of an indexed object (R may be grater than M).
     * @return  Weighted distance bound involved in similarity search.
     */
	private double minDistPoint(double q[], double v[]) {
    	
    	double minDist = 0.0;
    	double maxMetric = 0.0;
    	double distance;

    	// Iterate over all reference values
    	for (int m = 0; m < M; m++) {
    		maxMetric = 0.0;
    		// Take the max distance of q from all pivot-based embeddings per distance (Eq. 5)
    		for (int i = refs.getStartReference(m); i <= refs.getEndReference(m); i++) {
    			distance = refs.getMetric(m).diff(q[i], v[i]);
    			maxMetric = GeometryUtil.max(maxMetric, distance);  // max distance found among all reference points for this distance
    		}
    		minDist += W[m] * rescale(maxMetric, m);   // scaled distance
    	}

    	return (minDist / this.sumWeights);	
    }
    
	
	/**
	 * Compute the exact distance between the query and the entity with the given identifier.
	 * FIXME: Should we apply exponential decay on each metric prior of weighing them?
	 * @param oid  Unique identifier of an entity embedded and indexed in the tree.
	 * @param qPoint  Original (NOT embedded) attribute values of the query.
	 * @return  The exact weighted and scaled distance to be reported.
	 */
	private double exactDistance(Object oid, Map<String, Point> qPoint) {
		
		double distance = 0.0;
		// Iterate over all attribute values (distances)
    	for (int m = 0; m < M; m++) {
    		Point p = (Point) datasets.get(attrIdentifiers[m]).get(oid);
    		// Exclude calculations involving NaN ordinates
    		if (p.containsNaN())
    			continue;
    		// Weighted scaled distance between original attribute values of this entity and the respective ones in the query
    		distance += W[m] * rescale(refs.getMetric(m).calc(qPoint.get(refs.getAttribute(m)), p), m);   
    	}
    	
		return (distance / this.sumWeights);
	}
	
	
	/**
	 * Rescale (i.e., normalize) the given distance value with the scale factor specified for this attribute (metric).
	 * @param d  The original distance value computed using the distance metric.
	 * @param m  The m-th distance metric utilized in computations, i.e., for the m-th attribute.
	 * @return  The scale distance value.
	 */
	private double rescale(double d, int m) {
		
		return (d / scale[m]);
	}
	
}
