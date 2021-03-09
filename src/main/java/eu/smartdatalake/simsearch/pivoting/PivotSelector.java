package eu.smartdatalake.simsearch.pivoting;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.engine.IDistance;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Point;

/**
 * Selects reference points (a.k.a. pivots) among a given collection for a specific attribute using a distance.
 */
public class PivotSelector {

	Logger log;
	
	List<Point> points;
	IDistance distance;
	
	// Calculate the omni-coordinates that will be used for the indexed points in the RR*-tree
	double[][] distances;
	
	// Reference points (a.k.a. pivots) to be chosen
	List<Point> foci;
	
	/**
	 * Constructor
	 * @param distance  The distance metric specified for this type of points.
	 * @param points  The collection of data points to select pivots from.
	 * @param log  Handle to the log file for notifications and execution statistics.
	 */
	public PivotSelector(IDistance distance, List<Point> points, Logger log) {
		
		this.log = log;
		this.points = points;
		this.distance = distance;
	}
	
	
	/**
	 * From a collection of points, pick the one farthest from the given point.
	 * @param p  A point in the collection.
	 * @return  The point in the collection having the largest distance from p.
	 */
	private Point chooseFarthest(Point p) {
	    Point f = null;
	    double dist = 0.0;
	    double d;
	    int i = 0;
	    for (Point x: this.points) {
	    	if ((d = distance.calc(p, x)) > dist) {
	    		dist = d;
	    		f = x;
	    		// If the first reference point p has been chosen, then keep all its distances from the points in the collection
//	    		if (j > 0) {
//	    			distances[i][j-1] = d;
//	    		}
	    	}
	    	i++;
	    }
	    
	    // Calculate distances of all points in the collection from the newly chosen reference point f
//	    if (j > 0) {
	    	calcDistances(f, foci.size());
//	    }
	    return f;
	}

	
	/**
	 * Calculate distances of all points in the collection from the newly chosen reference point.
	 * @param f  The reference point (pivot).
	 * @param j  The number of foci (pivots) currently chosen.
	 */
	private void calcDistances(Point f, int j) {
		
	    int i = 0;
	    for (Point x: this.points) {
	    	distances[i][j] = distance.calc(f, x);
	    	i++;
	    }
	}
	
	
	/**
	 * From a collection of points, pick the one that minimizes the error from the edge specified by the two first foci.
	 * @param edge  Distance between the first two foci (pivots) already chosen.
	 * @return  A point in the collection to become the next chosen foci (pivot).
	 */
	private Point chooseByMinimalError(double edge) {
		Point f = null;	    
	    double error = Double.MAX_VALUE;
	    double e;
	    
	    int i = 0;
	    for (Point x: this.points) {
	    	if (!foci.contains(x)) {
	    		e = 0;
	    		for (int j = 0; j < foci.size(); j++) {
	    			e += Math.abs(edge - distances[i][j]);   // distances from previous pivot already computed
//	    			e += Math.abs(edge - distance.distance(x.mins(), foci.get(j).mins()));  
//	    			if (distances[i][j] - distance.distance(x.mins(), foci.get(j).mins()) > 0.001 )
//	    				System.out.println("Stored: " + distances[i][j] + " Calculated: " + distance.distance(x.mins(), foci.get(j).mins()) );
	    		}
	    		
		    	if (e < error) {
		    		error = e;
		    		f = x;
		    	}
	    	}
	    	
	    	i++;
	    }
	    
//	    System.out.println("Error: " + error);
	    
	    // Calculate distances of all points in the collection from the newly chosen reference point
	    calcDistances(f, foci.size());
	    
	    return f;
	}
	
	
	/**
	 * Implements the Hull of Foci algorithm and selects n reference points from the convex hull of the given collection of multi-dimensional points.
	 * @param n  Number of reference points (pivots) to choose.  
	 * @return  An array that, for each point in the data collection and each of the n chosen reference points, reports their distance.
	 */
	public double[][] embed(int n) {
		
//		System.out.println("Point collection size:" + points.size());
		
		// Initialize omni-coordinates for this attribute
		distances = new double[points.size()][n];
			
		// Initialize foci
		foci = new ArrayList<Point>();
		
		// Choose a random point from the collection 
		Random rand = new Random();
	    Point randomPoint = points.get(rand.nextInt(points.size()));
	    // CAUTION! This random point must not have NaN values in any of its ordinates, so that distances can be estimated properly
	    while (randomPoint.containsNaN()) {
	    	randomPoint = points.get(rand.nextInt(points.size()));
	    }
	      
	    // First pivot: Find farthest point f1 from the randomly chosen one
	    foci.add(chooseFarthest(randomPoint));
	    log.writeln("First pivot: " + foci.get(0).toString());

	    // Sometimes only one pivot will be selected, so remaining steps may be skipped
	    if (foci.size() < n) {
	    
		    // Second pivot: Find farthest point f2 from the first one
		    foci.add(chooseFarthest(foci.get(0)));
		    log.writeln("Second pivot: " + foci.get(1).toString());
		    
		    // Distance between the two first foci
		    double edge = distance.calc(foci.get(0), foci.get(1));
	
		    // Continue choosing foci until the specified count
		    while (foci.size() < n) {
		    	foci.add(chooseByMinimalError(edge));
		    	log.writeln("Next pivot: " + foci.get(foci.size()-1).toString());
		    }
	    }
	    
	    // Must return the embedding in order to be used for indexing in the tree
	    return distances;
	}
	
	
	/**
	 * Provides the chosen pivots, i.e., reference points selected across the convex hull of the input collection of multi-dimensional points
	 * @return  The chosen reference (pivot) values.
	 */
	public List<Point> getPivots() {
		return foci;
	}
	
}
