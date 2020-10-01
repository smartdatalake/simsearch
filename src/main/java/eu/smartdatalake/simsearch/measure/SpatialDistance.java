package eu.smartdatalake.simsearch.measure;

import org.locationtech.jts.geom.Geometry;

/**
 * Implements a spatial distance measure based on the Haversine distance.
 * FIXME: This class assumes that all locations always have a common CRS (i.e., WGS84).
 * @param <V>  Type variable to represent the values involved in the distance calculations (i.e., geometry locations).
 */
public class SpatialDistance<V> implements IDistance<V> {

	Geometry baseLocation;

	/**
	 * Constructor #1
	 * @param baseLocation  The geometry location specified in the search query.
	 */
	public SpatialDistance(Geometry baseLocation) {
		
		this.baseLocation = baseLocation;
	}
	
	/**
	 * Constructor #2
	 */
	public SpatialDistance() {
		
	}
	
	
	/**
	 * Returns the scaled Haversine distance of the given geometry from the fixed query location.
	 */
	@Override
	public double calc(V v) {
		// No need to check again for NULL values; already handled by DecayedSimilarity class
//		System.out.println("Spatial value:" + v.toString() + " Unscaled distance:" +  111.0 * baseLocation.distance((Geometry) v));

//		if (v != null)
			return baseLocation.distance((Geometry) v);
	}
	

	/**
	 * Returns the scaled Haversine distance between two POINT LOCATIONS.
	 */
	@Override
	public double calc(V v1, V v2) {
		// No need to check again for NULL values; already handled by DecayedSimilarity class
//		if ((v1 != null) && (v2 != null))
			return ((Geometry) v1).distance((Geometry) v2);
	}

}
