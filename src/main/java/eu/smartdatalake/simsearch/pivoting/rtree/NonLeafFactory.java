package eu.smartdatalake.simsearch.pivoting.rtree;

import java.util.List;

import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Geometry;

public interface NonLeafFactory<T, S extends Geometry> {

    NonLeaf<T, S> createNonLeaf(List<? extends Node<T, S>> children, Context<T, S> context);
}
