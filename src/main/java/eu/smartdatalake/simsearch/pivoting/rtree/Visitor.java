package eu.smartdatalake.simsearch.pivoting.rtree;

import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Geometry;

public interface Visitor<T, S extends Geometry> {

    void leaf(Leaf<T, S> leaf);

    void nonLeaf(NonLeaf<T, S> nonLeaf);

}
