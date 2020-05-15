package eu.smartdatalake.simsearch.csv.spatial;

import org.locationtech.jts.geom.Geometry;

/**
 * Representation for spatial locations along with their identifier.
 */
public class Location {

	public String key;
	public Geometry loc;
	
	/**
	 * Constructor
	 * @param id  The identifier of the object.
	 * @param g  The geometry (currently, a point location) of the object.
	 */
	public Location(String id, Geometry g) {
		this.key = id;
		this.loc = g;
	}
}
