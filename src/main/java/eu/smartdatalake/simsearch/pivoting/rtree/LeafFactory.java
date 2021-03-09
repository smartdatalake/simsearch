package eu.smartdatalake.simsearch.pivoting.rtree;

import java.util.List;

import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Geometry;

public interface LeafFactory<T, S extends Geometry> {
    Leaf<T, S> createLeaf(List<Entry<T, S>> entries, Context<T, S> context);
}
