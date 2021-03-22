package eu.smartdatalake.simsearch.pivoting.metrics;

import eu.smartdatalake.simsearch.engine.IDistance;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Point;

/**
 * Implements calculation of Chebyshev distance between two multi-dimensional points.
 * This method calculates a metric and is employed in pivot-based similarity search.
 * @param <V>  Type variable to represent the values involved in distance calculations (usually, points with double ordinates).
 */
public class ChebyshevDistance<V> implements IDistance<V> {
	
	double nanDistance;
	
	@Override
	public double calc(V v) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double calc(V v1, V v2) {

		// Cast each argument to a (possibly multi-dimensional) point
		return calc((Point) v1, (Point) v2);
	}

	@Override
	// Measure the absolute difference between two Chebyshev distances
    public double diff(double a, double b) {
    	return Math.abs(a - b);
    }
	

	/**
	 * Calculates the Chebyshev distance between two equi-sized arrays of double values.
	 * @param a  An array of double values representing the ordinates of a multi-dimensional point.
	 * @param b  An array of double values representing the ordinates of another multi-dimensional point.
	 * @return  A double value measuring the Chebyshev distance between the two arrays.
	 */
    public double calc(double[] a, double[] b) {
    	
    	if (a.length != b.length)
    		throw new IllegalArgumentException("Vectors must have the same size");
    	
    	// Calculate Chebyshev distance
    	double max = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = Math.abs(a[i] - b[i]);
            if (diff > max)
            	max = diff;
        }
        return max;
    }

    
    /**
     * Calculates the Chebyshev distance between two multi-dimensional points.
     * @param p1  A multi-dimensional point with double values as ordinates.
     * @param p2  Another multi-dimensional point with double values as ordinates.
     * @return  A double value measuring the Chebyshev distance between the two points.
     */
    public double calc(Point p1, Point p2) {
    	
    	// Special handling of NaN ordinates in the given points
    	if (p1.containsNaN() || p2.containsNaN())
    		// A default (e.g., average or maximal) distance assumed if an operand is NULL (NaN)
    		return nanDistance;
    	
    	return calc(p1.mins(), p2.mins());
    }

    
	@Override
	public void setNaNdistance(double d) {
			
		nanDistance = d;
	}

}
