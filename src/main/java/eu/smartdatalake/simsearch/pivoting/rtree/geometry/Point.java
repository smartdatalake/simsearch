package eu.smartdatalake.simsearch.pivoting.rtree.geometry;

import java.util.List;

import eu.smartdatalake.simsearch.pivoting.rtree.geometry.internal.PointDouble;

public interface Point extends Rectangle {

    double[] mins();
    
    default double[] maxes() {
        return mins();
    }
    
    default double[] values() {
        return mins();
    }
    
    public static Point create(double... x) {
        return PointDouble.create(x);
    }
    
    public static Point create(List<? extends Number> x) {
        double[] a = new double[x.size()];
        for (int i = 0; i< x.size(); i++) {
            a[i] = x.get(i).doubleValue();
        }
        return create(a);
    }

    /**
     * Extra functionality for handling NaN values in ordinates
     */
    public default boolean containsNaN() {
    	
    	for (double x: mins()) {
    		if (Double.isNaN(x))
    			return true;
    	}
    	
    	return false;
    }
}
