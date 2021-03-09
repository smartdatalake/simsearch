package eu.smartdatalake.simsearch.pivoting.rtree;

import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Geometry;
import eu.smartdatalake.simsearch.pivoting.rtree.internal.FactoryDefault;

public interface Factory<T, S extends Geometry>
        extends LeafFactory<T, S>, NonLeafFactory<T, S>, EntryFactory<T,S> {
    
    public static <T, S extends Geometry> Factory<T, S> defaultFactory() {
        return FactoryDefault.instance();
    }
}
