package eu.smartdatalake.simsearch.pivoting.metrics;

import eu.smartdatalake.simsearch.engine.IDistance;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Point;

/**
 * Implements calculation of Haversine distance between two 2-dimensional points.
 * This method calculates a metric and is employed in pivot-based similarity search.
 * @param <V>  Type variable to represent the values involved in distance calculations (usually, points with double ordinates).
 */
public class HaversineDistance<V> implements IDistance<V> {

	double earthRadius = 6372800;  // in meters
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
	// Measure the absolute difference between two Haversine distances
	public double diff(double a, double b) {
		return Math.abs(a - b);
	}

	
	/**
	 * Calculates the Haversine distance between two pairs of double values representing (lon, lat) coordinates.
	 * @param a  An array of double values representing the (lon, lat) ordinates of a 2-dimensional point.
	 * @param b  An array of double values representing the (lon, lat) ordinates of another 2-dimensional point.
	 * @return  A double value measuring the Haversine distance between the two arrays.
	 */
	public double calc(double[] a, double[] b) {

        double x1 = Math.toRadians(a[0]);
        double y1 = Math.toRadians(a[1]);
        double x2 = Math.toRadians(b[0]);
        double y2 = Math.toRadians(b[1]);

        // Compute distance using Haversine formula
        double d = Math.pow(Math.sin((x2-x1)/2), 2)
                 + Math.cos(x1) * Math.cos(x2) * Math.pow(Math.sin((y2-y1)/2), 2);

        // great circle distance in radians
        double angle = 2 * Math.asin(Math.min(1, Math.sqrt(d)));
        
        // Return distance in meters
//        return earthRadius * angle;
        
        // Return distance in decimal degrees
        return Math.toDegrees(angle);       
	}

	
    /**
     * Calculates the Haversine distance between two multi-dimensional points.
     * @param p1  A multi-dimensional point with double values as ordinates.
     * @param p2  Another multi-dimensional point with double values as ordinates.
     * @return  A double value measuring the Haversine distance between the two points.
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
