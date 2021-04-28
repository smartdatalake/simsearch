package eu.smartdatalake.simsearch.pivoting;

import java.util.Arrays;
import java.util.Map;

import eu.smartdatalake.simsearch.pivoting.rtree.MultiMetricSimilaritySearch;
import eu.smartdatalake.simsearch.pivoting.rtree.Node;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Geometry;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Point;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.internal.GeometryUtil;

/**
 * Auxiliary class that handles scaling of distances in pivot-based similarity search.
 */
public class ScaleFactors {

	double[] scale;
	int M;   				// Number of distances
	MetricReferences ref;
	
	/**
	 * Constructor
	 * @param R  Representation of all reference (pivot) values for all distances.
	 * @param m  Number of distances (i.e., attributes involved in the index).
	 */
	public ScaleFactors(MetricReferences R, int m) {
		
		ref = R;
		M = m;
		scale = new double[M];	
		Arrays.fill(scale, 1.0);  // Initialization
	}
	
	/**
	 * Calculates the scale factors used in normalizing distance values for the various metrics.
	 * This method calculates the distance of k-nearest neighbor separately per query attribute value (ignoring the values in the rest).
	 * @param datasets  Dictionary of input datasets used in creating the underlying index.
	 * @param attrIdentifiers  Attribute identifiers per distance metric, each corresponding to the input datasets.
	 * @param root  Root node of the RR*-tree index that holds the multi-dimensional embeddings of the data points.
	 * @param q  The R-dimensional embedding of the query point.
	 * @param k  The number of results to return as the top-k most similar to the query.
	 */
	public <T, S extends Geometry, D> void setScale2KNN(Map<String, Map<?,?>> datasets, String[] attrIdentifiers, Node<T, S> root, Point q, int k) {
		
		// Estimate scaling factor for each distance
        for (int m = 0; m < M; m++) {
        	// Apply no scaling
        	double[] tmpScale = new double[M];
	    	Arrays.fill(tmpScale, 1);
	    	// All weights set to 0.0, except the m-th one
	    	double[] tmpWeights = new double[M];
	    	Arrays.fill(tmpWeights, 0.0);
	    	tmpWeights[m] = 1.0;
	    	MultiMetricSimilaritySearch simQuery = new MultiMetricSimilaritySearch(datasets, attrIdentifiers, ref, tmpWeights, tmpScale, null);
//	        List<NearestEntry<Object, Point, Double>> simResults = (List<NearestEntry<Object, Point, Double>>) simQuery.search(tree.root().get(), q, k);
//	        double d = simResults.get(simResults.size()-1).distance();
        	double d = simQuery.getScaleFactor(root, q, k);
        	System.out.println(m + "-th scale factor: " + d);
        	scale[m] = (d > 0) ? d : 1.0;
        }
	}
	
	/**
	 * Calculates the scale factors used in normalizing distance values for the various metrics.
	 * This method uses the range of values per attribute in the data for the normalization.
	 * @param mins  R-dimensional point having as ordinates the minimum values per embedding among all indexed data points.
	 * @param maxes R-dimensional point having as ordinates the maximum values per embedding among all indexed data points.
	 */
	public void setScale2MaxRange(double[] mins, double[] maxes) {
		
		double maxRange;
		double distance;
		// Iterate over all reference values
    	for (int m = 0; m < M; m++) {
    		maxRange = 0.0;
    		// Take the max range among reference values per distance
    		for (int i = ref.getStartReference(m); i <= ref.getEndReference(m); i++) {
    			distance = maxes[i] - mins[i];
    			maxRange = GeometryUtil.max(maxRange, distance);
    		}
    		scale[m] = (maxRange > 0) ? maxRange : 1.0;
    	}
	}
	
	/**
	 * Provides the scale factors (one per distance).
	 * @return  Array of m double values to be used as denominators when normalizing distance values per distance.
	 */
	public double[] getAll() {
		
		return scale;
	}
	
	/**
	 * Provides the scale factors to be used for the specified distance.
	 * @param i  The i-th distance in the index (associated with an attribute).
	 * @return  The scale factor to be be used in normalizing distances for the given attribute (distance).
	 */
	public double get(int i) {
		
		return scale[i];
	}
	
}
