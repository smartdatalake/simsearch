package eu.smartdatalake.simsearch.csv.spatial;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.STRtree;

import eu.smartdatalake.simsearch.csv.Index;

/**
 * Implements an R-tree index based on the JTS STRtree for use in spatial similarity search queries.
 * @param <K>  Type variable representing the keys of the indexed objects.
 * @param <V>  Type variable representing the values of the indexed objects.
 */
public class RTree<K extends Comparable<? super K>, V> implements Index<Object, Object> {

	public STRtree idx;
	
	/**
	 * Constructor instantiates an STRtree available in the Java Topology Suite (JTS).
	 */
	public RTree() {
		idx = new STRtree();   
	}
	
	/**
	 * Inserts the MBR of the given geometry into the R-tree.
	 * @param g  The geometry specification.
	 * @param id  The id of the indexed object.
	 */
	public void insert(Location p) {
		Envelope env = p.loc.getEnvelopeInternal();
		idx.insert(env, p);
	}
	
	/**
	 * Returns the depth of the R-tree index.
	 * @return  The depth (=height) of the tree.
	 */
	public int getDepth() {
		return idx.depth();
	}
	
	/**
	 * Returns the number of objects indexed in the R-tree.
	 * @return  The number of indexed geometries.
	 */
	public int getItems() {
		return idx.size();
	}
	
	/**
	 * Builds the index once insertion of all geometries is complete.
	 */
	public void finalize() {
		idx.build();
	}
	
	/**
	 * Provides the Minimum Bounding Rectangle (MBR) of all geometry locations indexed in the R-tree.
	 * @return An envelope representing the MBR.
	 */
	public Envelope getMBR() {
		return (Envelope) idx.getRoot().getBounds();
	}
	
	/**
	 * Provides the k Nearest Neighbors to the specified query location.
	 * @param qryLoc  The query location.
	 * @param k  The number of closest locations to fetch from the underlying R-tree.
	 * @return  An array of the k locations (indexed in the R-tree).
	 */
	public Object[] getKNearestNeighbors(V qryLoc, int k) {
		
		Location p = (Location) qryLoc;
//		System.out.println("KNN query: " + p.loc.toText() + " envelope:" + p.loc.getEnvelopeInternal());
		return idx.nearestNeighbour(p.loc.getEnvelopeInternal(), p, new LocationItemDistance(), k);
	}
}
