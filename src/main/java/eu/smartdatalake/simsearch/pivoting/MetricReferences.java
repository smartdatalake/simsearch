package eu.smartdatalake.simsearch.pivoting;

import eu.smartdatalake.simsearch.engine.IDistance;
import eu.smartdatalake.simsearch.pivoting.metrics.EuclideanDistance;
import eu.smartdatalake.simsearch.pivoting.metrics.HaversineDistance;
import eu.smartdatalake.simsearch.pivoting.metrics.JaccardDistance;
import eu.smartdatalake.simsearch.pivoting.metrics.ManhattanDistance;

/**
 * Representation of distance metrics and number of reference (pivot) values per attribute.
 */
public class MetricReferences {

	String[] attributes;	// Names of the attributes
	// Arrays of consecutive serial numbers indicating reference (pivot) positions per attribute
	int[] refStart;   		// First pivot per attribute
	int[] refEnd;			// Last pivot per attribute
	IDistance[] metrics;   	// Metrics per attribute
	int[] dimensions;    	// Dimensionality of points (i.e., number of ordinates) per attribute

	/**
	 * Constructor
	 * @param M  number of metrics
	 */
	public MetricReferences(int M) {
		
		attributes = new String[M];
		refStart = new int[M];
		refEnd = new int[M];
		metrics = new IDistance[M];
		dimensions = new int[M];
	}
	
	/**
	 * Sets the starting reference (pivot position) for a given distance in multi-dimensional rectangles
	 * @param m  The m-th item (i.e., distance) involved in the search
	 * @param i  The i-th reference in multi-dimensional rectangles
	 */
	public void setStartReference(int m, int i) {
		refStart[m] = i;
	}

	/**
	 * Sets the last reference (pivot position) for a given distance in multi-dimensional rectangles
	 * @param m  The m-th item (i.e., distance) involved in the search
	 * @param i  The i-th reference in multi-dimensional rectangles
	 */
	public void setEndReference(int m, int i) {
		refEnd[m] = i;
	}
	
	/**
	 * Provides the starting reference (pivot position) for a given distance metric in multi-dimensional rectangles
	 * @param m  The m-th item (i.e., distance metric) involved in the search
	 * @return  The starting reference used for a given distance in multi-dimensional rectangles
	 */
	public int getStartReference(int m) {
		return refStart[m];
	}
	
	/**
	 * Provides the last reference (pivot position) for a given distance in multi-dimensional rectangles
	 * @param m  The m-th item (i.e., distance metric) involved in the search
	 * @return  The last reference used for a given distance in multi-dimensional rectangles
	 */
	public int getEndReference(int m) {
		return refEnd[m];
	}
	
	/**
	 * Counts the number of reference values (pivots) used for a given distance in multi-dimensional rectangles
	 * @param m  The m-th item (i.e., distance metric) involved in the search
	 * @return  The count of reference values used for the given distance
	 */
	public int countDimensionReferenceValues(int m) {
		return (refEnd[m] - refStart[m] + 1);
	}
	
	/**
	 * Counts the number M of metrics employed
	 * @return  An integer indicating the number of metrics used in the search.
	 */
	public int countMetrics() {
		return refStart.length;
	}
	
	/**
	 * Count the total number of reference values (pivots) across all dimensions.
	 * @return   An integer indicating the total number of reference values (pivots).
	 */
	public int totalReferenceValues() {
		int c = 0;
		for (int m = 0; m < countMetrics(); m++)
			c += countDimensionReferenceValues(m);
		return c;
	}
	
	/**
	 * Specifies the distance metric to be used for a given attribute.
	 * @param m  The m-th item among those designated as queryable attributes.
	 * @param metricName  The name of the distance metric to be applied.
	 */
	public void setMetric(int m, String metricName) {
		
		IDistance distance = null;
		switch (metricName) {
        case "Manhattan":
        	distance = new ManhattanDistance();
            break;
        case "Euclidean":
        	distance = new EuclideanDistance();
            break;
        case "Haversine":
        	distance = new HaversineDistance();
            break;
        case "Jaccard":
        	distance = new JaccardDistance();
            break;
        // TODO: Handle other metrics ...
        // Assuming Manhattan as the default
        default:
        	distance = new ManhattanDistance();
            break;
		}
		metrics[m] = distance;
	}
	
	/**
	 * Specifies the distance metric to be used for a given attribute.
	 * @param m  The m-th item among those designated as queryable attributes.
	 * @param metric  The distance metric to be applied.
	 */
	public void setMetric(int m, IDistance metric) {
		metrics[m] = metric;
	}
	
	/**
	 * Provides the distance metric being applied on a given attribute.
	 * @param m  The m-th item among those designated as queryable attributes.
	 * @return  A distance metric.
	 */
	public IDistance getMetric(int m) {
		return metrics[m];
	}
	
	/**
	 * Specifies the attribute name to be associated with a distance metric.
	 * @param m  The m-th item among those designated as queryable attributes.
	 * @param attrName  The attribute name.
	 */
	public void setAttribute(int m, String attrName) {
		attributes[m] = attrName;
	}
	
	/**
	 * Provides the attribute name associated with a distance metric.
	 * @param m  The m-th item among those designated as queryable attributes.
	 * @return  The attribute name.
	 */
	public String getAttribute(int m) {
		return attributes[m];
	}
	
	/**
	 * Finds the ordinal number used internally for the given attribute name.
	 * @param attr  An attribute name of the input dataset.
	 * @return  An integer that specifies the position in the arrays used for the given attribute.
	 */
	public int getAttributeOrder(String attr) {
		for (int i = 0; i < attributes.length; i++) {
			if (attributes[i].equals(attr))
				return i;
		}
		return -1;
	}

	/**
	 * Provides the dimensionality of points for the m-th attribute.
	 * @param m  The m-th item among those designated as queryable attributes.
	 * @return  The number of ordinates in point representation for the given attribute.
	 */
	public int getDimension(int m) {
		return dimensions[m];
	}

	
	/**
	 * Provides the dimensionality of points for the given attribute name.
	 * @param attr   An attribute name of the input dataset.
	 * @return  The number of ordinates in point representation for the given attribute.
	 */
	public int getDimension(String attr) {
		for (int i = 0; i < attributes.length; i++) {
			if (attributes[i].equals(attr))
				return dimensions[i];
		}
		return 0;
	}
	
	/**
	 * Specifies the dimensionality of points for the m-th attribute
	 * @param m  The m-th item among those designated as queryable attributes.
	 * @param d  The number of ordinates in point representation for the given attribute.
	 */
	public void setDimension(int m, int d) {
		this.dimensions[m] = d;
	}
	
}
