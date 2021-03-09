package eu.smartdatalake.simsearch.pivoting.rtree.internal;

import java.util.List;

//import com.github.davidmoten.guavamini.Preconditions;
import eu.smartdatalake.simsearch.pivoting.rtree.Context;
import eu.smartdatalake.simsearch.pivoting.rtree.Entry;
import eu.smartdatalake.simsearch.pivoting.rtree.Node;
import eu.smartdatalake.simsearch.pivoting.rtree.NonLeaf;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Geometry;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Rectangle;

public final class NonLeafDefault<T, S extends Geometry> implements NonLeaf<T, S> {

    private final List<? extends Node<T, S>> children;
    private final Rectangle mbr;
    private final Context<T, S> context;

    public NonLeafDefault(List<? extends Node<T, S>> children, Context<T, S> context) {
    	
//    	Preconditions.checkArgument(!children.isEmpty());
        if (children.isEmpty())
            throw new IllegalArgumentException();
        
        this.context = context;
        this.children = children;
        this.mbr = Util.mbr(children);
    }

    @Override
    public Geometry geometry() {
        return mbr;
    }

    @Override
    public int count() {
        return children.size();
    }

    @Override
    public List<Node<T, S>> add(Entry<? extends T, ? extends S> entry) {
        return NonLeafHelper.add(entry, this);
    }

    @Override
    public NodeAndEntries<T, S> delete(Entry<? extends T, ? extends S> entry, boolean all) {
        return NonLeafHelper.delete(entry, all, this);
    }

    @Override
    public Context<T, S> context() {
        return context;
    }

    @Override
    public Node<T, S> child(int i) {
        return children.get(i);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Node<T, S>> children() {
        return (List<Node<T, S>>) children;
    }

    @Override
    public String toString() {
        return "NonLeafDefault [mbr=" + mbr + ", children=" + children + "]";
    }
    
}