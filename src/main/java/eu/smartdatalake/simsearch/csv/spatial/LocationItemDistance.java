package eu.smartdatalake.simsearch.csv.spatial;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.ItemBoundable;
import org.locationtech.jts.index.strtree.ItemDistance;

/**
 * An {@link ItemDistance} function for 
 * items which are {@link Geometry}s,
 * using the {@link Geometry#distance(Geometry)} method.
 * <p>
 * To make this distance function suitable for
 * using to query a single index tree,
 * the distance metric is <i>anti-reflexive</i>.
 * That is, if the two arguments are the same Geometry object,
 * the distance returned is {@link Double.MAX_VALUE}.
 *
 */
public class LocationItemDistance implements ItemDistance {
	
  /**
   * Computes the distance between two {@link Geometry} items,
   * using the {@link Geometry#distance(Geometry)} method.
   * 
   * @param item1 an item which is a Location
   * @param item2 an item which is a Location
   * @return the distance between the geometries of the given locations
   * @throws ClassCastException if either item is not a Geometry
   */
  public double distance(ItemBoundable item1, ItemBoundable item2) {
    if (item1 == item2) 
    	return Double.MAX_VALUE;
    
    Location l1 = (Location) item1.getItem();
    Location l2 = (Location) item2.getItem();
    
    return l1.loc.distance(l2.loc);    
  }
}
