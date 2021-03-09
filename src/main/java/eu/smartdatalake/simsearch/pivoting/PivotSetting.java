package eu.smartdatalake.simsearch.pivoting;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.engine.IDistance;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Point;

/**
 * Chooses the number of reference (pivot) values per attribute; NOT the actual pivots.
 * Greedily adding extra pivots to chosen distances according their pruning potential.
 * CAUTION! The pivots randomly chosen by this method will NOT be used in RR*-tree construction.
 * Actual pivots will be picked by the PivotSelector using the "Hull of Foci" algorithm.
 */
public class PivotSetting {

	Logger log = null;
	
	private int M;  	// Number of distances
	private int R;   	// Total number of reference (pivot) values to select
	private double D; 	// Total number of objects in the dataset
	
	IDistance[] distances;   		// Distance metrics; one per attribute
	List<List<Point>> objects;   	// Objects used for choosing a suitable number of pivots
	List<List<Point>> pivots;		// One list of (randomly chosen) pivots per attribute
	List<Double> epsilon;			// Indicative distance values for calculating pruning potential per attribute
	
	/**
	 * Constructor
	 * @param m  Number of distance metrics (attributes).
	 * @param r  Total number of reference points (pivots) for all attributes.
	 * @param metrics  The array of the actual distance metrics (one per queryable attribute).
	 * @param sample  One collection of sample points per attribute.
	 * @param log  Handle to the log file for notifications and execution statistics.
	 */
	public PivotSetting(int m, int r, IDistance[] metrics, List<List<Point>> sample, Logger log) {
		
		this.log = log;
		this.M = m;
		this.R = r;
		this.distances = metrics;
		
		// The given sample is used for estimating the number of pivot values per distance
		this.objects = sample;
		this.D = 1.0 * this.objects.get(0).size();   // FIXME: Assuming the same count of objects per distance metric (attribute)
		
		// Initialization of pivots
		this.pivots = new ArrayList<List<Point>>(M);		
     	for (int i = 0; i < M; i++) {
     		this.pivots.add(new ArrayList<Point>());
     	}     	
	}
	
	
	/**
	 * Estimate an espilon threshold per attribute (distance) as the average distance between nearest neighbors
	 * @param m  The m-th distance to be applied in distance computations
	 * @return  An average distance threshold.
	 */
	private double epsEstimation(int m) {

		double sum = 0.0;
		double d;
		
		// Pairwise distance computations to choose the smallest one concerning the nearest neighbor per point
		for (Point p: objects.get(m)) {
			double nnDist = Double.MAX_VALUE;
			for (Point q: objects.get(m)) {
				if (p != q) {
					d = distances[m].calc(p, q);
					if (d < nnDist)
						nnDist = d;
				}
			}
			sum += nnDist;  // Sum up this nearest neighbor distance
		}

		// Return average of distances for this attribute (distance metric)
		return (sum / D);	
	}
	
	/**
	 * Indicator function of whether two given objects are within distance epsilon based on the current pivots for a distance metric
	 * @param m  The m-th distance metric involved.
	 * @param setPivots  A collection of pivots specified for this distance.
	 * @param a  An object from the sample.
	 * @param b  another object from the sample.
	 * @return  Indicator 1 if distance bounds can be used for pruning; 0, otherwise.
	 */
	private int indicator(int m, List<Point> setPivots, Point a, Point b) {		
		
		// Examine all current pivots for this distance metric
		// CAUTION: Number of pivots per distance may increase
		for (Point r: setPivots) {
			if ((Math.abs(distances[m].calc(a, r) - distances[m].calc(b, r))) > epsilon.get(m)) {
				return 1;
			}
			if (distances[m].calc(a, r) + distances[m].calc(b, r) <= epsilon.get(m)) {
				return 1;
			}
		}
		
		return 0;   // default indicator	
	}
	
	
	/**
	 * Calculate pruning potential for a given distance metric (attribute).
	 * @param m  The m-th distance metric involved.
	 * @param setPivots  A collection of pivots specified for this distance metric.
	 * @return  The estimated pruning potential.
	 */
	private double epsPruningPotential(int m, List<Point> setPivots) {		
		
		double potential = 0.0;
		
		for (Point a: objects.get(m)) {
			for (Point b: objects.get(m)) {
				if (a != b) {
					potential += indicator(m, setPivots, a, b) / (D * D);
				}
			}
		}
		
		return potential;
	}
	
	
	/**
	 * Provides a candidate pivot randomly chosen for a given distance metric (attribute).
	 * @param m  The m-th distance metric involved.
	 * @return  A candidate pivot (reference point) for the given distance metric.
	 */
	private Point getCandidatePivot(int m) {
		Point seedPivot = null;
		while (true) {
			Random rand = new Random();
			seedPivot = objects.get(m).get(rand.nextInt(objects.get(m).size()));
			if (!pivots.get(m).contains(seedPivot))
				break;
		}
		
		return seedPivot;
	}
	
	
	/**
	 * Count the total number of pivots currently assigned across all distance metrics (queryable attributes).
	 * @return  The count of all pivots (reference points).
	 */
	private int totalPivots() {
		
		int cnt = 0;
		for (int m = 0; m < M; m++) {
			cnt += pivots.get(m).size();
		}
		
		return cnt;
	}
	
	/**
	 * Greedily adding extra pivots to chosen distances according their pruning potential
	 * @return  An array with the number of pivots suggested per distance metric.
	 */
	public int[] greedyMaximization() {
		
		int[] pivotsPerMetric = new int[M];
		
		// Estimate indicative distance thresholds per distance based on the given sample of objects
		// CAUTION! Keep these distances as scale factors in actual distance computations at query time
		epsilon = new ArrayList<Double>(M);
		for (int m = 0; m < M; m++) {
			epsilon.add(epsEstimation(m));
//    		System.out.println(m + "-th epsilon:" + epsilon.get(m));
    	}

		// Initialize current and test potential per distance
		List<Double> curPotentials = new ArrayList<Double>(M);
		List<Double> testPotentials = new ArrayList<Double>(M);
		for (int m = 0; m < M; m++) {
			curPotentials.add(0.0);   // Initially zero potential
			testPotentials.add(0.0);  // Initially zero potential
		}
		
		log.writeln("Pivots to select: " + R);
		int i = -1;
		// Iterate until R pivots have been picked
		while (totalPivots() < R) {
			
			for (int m = 0; m < M; m++) {
				// Need to re-evaluate potential only for distances with updated pivots
				if ((i < 0) || ( i == m)) {
					double p = 0.0;
					for (Point o: objects.get(m)) {
						if (!pivots.get(m).contains(o)) {
							pivots.get(m).add(o);
							p += epsPruningPotential(m, pivots.get(m));
							pivots.get(m).remove(o);
						}
					}
					// Potential when the pivot to be chosen comes from the m-th distance
					testPotentials.set(m, p / (1.0 * D) - curPotentials.get(m));
				}
				log.writeln(m + "-th potential: " + testPotentials.get(m));
			}
		
			// Find the distance that maximized the potential
			i = testPotentials.indexOf(Collections.max(testPotentials));
//			log.writeln("Picked " + i + "-th distance " + distances[i].getClass().getSimpleName());
			
			// Choose a random point from the collection as the pivot for this distance
			// CAUTION! These pivots will NOT be used in RR*-tree construction
			Point c = getCandidatePivot(i);
			pivots.get(i).add(c);
			
			// Potential based on the updated pivots for this distance
			curPotentials.set(i, epsPruningPotential(i, pivots.get(i)));
			
			log.writeln("Picked random pivot: " + c.toString() + " for " + i + "-th distance " + distances[i].getClass().getSimpleName());
		}
		
		// Return the count of pivots per distance
		for (int m = 0; m < M; m++) {
			pivotsPerMetric[m] = pivots.get(m).size();
		}
		
		log.writeln("Choosing pivot count per distance concluded. Total number of pivots: " + totalPivots());
		
		return pivotsPerMetric;		
	}
	
	
	/**
	 * Provides the indicative distance thresholds per distance metric (attribute).
	 * @return  An array of non-zero doubles as an estimation of distances between nearest neighbors per distance.
	 */
	public double[] getEpsilonThresholds() {
		
    	Double[] epsArray = epsilon.toArray(new Double[0]);
    	
    	return Stream.of(epsArray).mapToDouble(Double::doubleValue).toArray();
	}
	
}
