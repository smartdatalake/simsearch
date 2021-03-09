package eu.smartdatalake.simsearch.pivoting.rtree;

import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Geometry;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.HasGeometry;
import eu.smartdatalake.simsearch.pivoting.rtree.internal.NearestEntryDefault;

public interface NearestEntry<T, S extends Geometry, D> extends HasGeometry {

    T value();

    @Override
    S geometry();
    
	D distance();
	
    public static <T, S extends Geometry, D> NearestEntry<T, S, D> entry(T object, S geometry, D distance) {
        return NearestEntryDefault.nearestEntry(object, geometry, distance);
    }
    
}