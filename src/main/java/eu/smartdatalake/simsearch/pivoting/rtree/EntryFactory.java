package eu.smartdatalake.simsearch.pivoting.rtree;

import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Geometry;

public interface EntryFactory<T,S extends Geometry> {
    Entry<T,S> createEntry(T value, S geometry);
}
