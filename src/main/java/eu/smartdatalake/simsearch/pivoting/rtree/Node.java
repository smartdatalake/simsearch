package eu.smartdatalake.simsearch.pivoting.rtree;

import java.util.List;

import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Geometry;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.HasGeometry;
import eu.smartdatalake.simsearch.pivoting.rtree.internal.NodeAndEntries;

public interface Node<T, S extends Geometry> extends HasGeometry {

    List<Node<T, S>> add(Entry<? extends T, ? extends S> entry);

    NodeAndEntries<T, S> delete(Entry<? extends T, ? extends S> entry, boolean all);

    int count();

    Context<T, S> context();
    
    boolean isLeaf();

}
