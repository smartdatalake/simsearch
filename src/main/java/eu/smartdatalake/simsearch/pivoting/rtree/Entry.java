package eu.smartdatalake.simsearch.pivoting.rtree;

import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Geometry;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.HasGeometry;
import eu.smartdatalake.simsearch.pivoting.rtree.internal.EntryDefault;

public interface Entry<T, S extends Geometry> extends HasGeometry {

    T value();

    @Override
    S geometry();
    
    public static <T, S extends Geometry> Entry<T,S> entry(T object, S geometry) {
        return EntryDefault.entry(object, geometry);
    }

}