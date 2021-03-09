package eu.smartdatalake.simsearch.pivoting.rtree;

import java.util.ArrayList;
import java.util.List;

import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Geometry;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Point;
import eu.smartdatalake.simsearch.pivoting.rtree.internal.Comparators;
import eu.smartdatalake.simsearch.pivoting.rtree.internal.NearestEntryDefault;
import eu.smartdatalake.simsearch.pivoting.rtree.internal.util.CandidatePriorityQueue;


/**
 * Implements k-nearest neighbor search over a multi-dimensional R-tree.
 * It employs the distance browsing method described in G.R. Hjaltason and H. Samet: "Distance browsing in spatial databases" (https://dl.acm.org/doi/10.1145/320248.320255).
 */
public final class DistanceBrowsing {

	/**
	 * Constructor
	 */
    private DistanceBrowsing() {
        // prevent instantiation
    }
    

    /**
     * Recursively examining whether to continue searching the subtree of a node; tree traversal should start from the root.
     * @param node  Node of the tree currently searched during tree traversal. 
     * @param q  The multi-dimensional representation of the query point; Actually, the MBR of the query location is used as focus in the search.
     * @param k  The number k of entities to fetch as most similar to the query.
     * @return  A collection of k entities nearest to the query point.
	 */
    static <R, T, S extends Geometry, D> Iterable<NearestEntry<T, S, Double>> search(Node<T, S> node, Point q, int k) {
    	
    	// Collector of qualifying results
    	List<NearestEntry<T, S, Double>> results = new ArrayList<NearestEntry<T, S, Double>>();
    	
    	// Priority queue of visited nodes and entries
    	CandidatePriorityQueue<Candidate<Object, Double>> Q = new CandidatePriorityQueue<Candidate<Object, Double>>(Comparators.ascendingDistance());
    	
    	Candidate<Object, Double> element;
    	Node<?, ?> n;
    	
    	// Add root node to the queue with its distance from q
    	Q.enqueue(new Candidate<Object, Double>(node, q.distance(node.geometry().mbr())));
//    	System.out.println("Pushing root with distance: " + q.distance(node.geometry().mbr()));
    	
    	int cnt = 0;
    	while (!Q.isEmpty()) {
    		// Get the next element from the priority queue: it can be either a node (leaf or internal) or an entry (i.e., geometry object)
    		element = Q.dequeue();   		
    		// Handle types of nodes and report results
    		if (element.node() instanceof Entry<?,?>) {  // This is an entry (i.e., a geometry object) 			
    			Entry<?, ?> e = (Entry<?, ?>) element.node();
    			if ((!Q.isEmpty()) && (element.distance() > Q.peek().distance())) {
    				// Actually, the distance must be calculated between geometries, not based on their MBRs
    				// Since we deal with multi-dimensional points only, these are equivalent
//    				System.out.println("Pushing entry AGAIN with distance: " + element.distance() + " First distance:" + Q.peek().distance());
    				Q.enqueue(new Candidate<Object, Double>(e, element.distance()));
    			}
    			else 
    			{  // Report next result
    				results.add(new NearestEntryDefault<T, S, Double>((T)e.value(), (S) e.geometry(), element.distance()));
//    				System.out.println("kNN with distance: " + p.distance(e.geometry().mbr()));
    				cnt++;
    			}	
    		}
    		else {   // This is a node, either internal or leaf			
    			n = (Node<?, ?>) element.node();
	    		if (n.isLeaf()) {  	// Leaf node
	    			// Push all its entries into the queue according to their distances from the query point
	    			for (Entry<?, ?> e : ((Leaf<?, ?>) n).entries()) {
//	    				System.out.println("Pushing entry with distance: " + q.distance(e.geometry().mbr()));
	    				Q.enqueue(new Candidate<Object, Double>(e, q.distance(e.geometry().mbr())));
	    			}
	    		}
	    		else {   			// Internal node
	    			// Push all its children into the queue according to their distances from the query point
	    			for (Node<?, ?> c : ((NonLeaf<?, ?>) n).children()) {
//	    				System.out.println("Pushing node with distance: " + q.distance(c.geometry().mbr()));
	    				Q.enqueue(new Candidate<Object, Double>(c, q.distance(c.geometry().mbr())));
	    			}
	    		}
    		}
    		
    		// Stop searching once k-NN entries have been collected
    		if (cnt >= k)
    			break;
    	}
    	
//    	System.out.println("Max number of elements ever held in the priority queue:" + Q.getMaxElements());
    	      
        return results;
    }
    
}
