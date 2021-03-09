package eu.smartdatalake.simsearch.pivoting.rtree.internal;

import java.util.List;

import eu.smartdatalake.simsearch.pivoting.rtree.Context;
import eu.smartdatalake.simsearch.pivoting.rtree.Entry;
import eu.smartdatalake.simsearch.pivoting.rtree.Factory;
import eu.smartdatalake.simsearch.pivoting.rtree.Leaf;
import eu.smartdatalake.simsearch.pivoting.rtree.Node;
import eu.smartdatalake.simsearch.pivoting.rtree.NonLeaf;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Geometry;

public final class FactoryDefault<T, S extends Geometry> implements Factory<T, S> {

    private static class Holder {
        private static final Factory<Object, Geometry> INSTANCE = new FactoryDefault<Object, Geometry>();
    }

    @SuppressWarnings("unchecked")
    public static <T, S extends Geometry> Factory<T, S> instance() {
        return (Factory<T, S>) Holder.INSTANCE;
    }

    @Override
    public Leaf<T, S> createLeaf(List<Entry<T, S>> entries, Context<T, S> context) {
        return new LeafDefault<T, S>(entries, context);
    }

    @Override
    public NonLeaf<T, S> createNonLeaf(List<? extends Node<T, S>> children, Context<T, S> context) {
        return new NonLeafDefault<T, S>(children, context);
    }

    @Override
    public Entry<T, S> createEntry(T value, S geometry) {
        return Entry.entry(value, geometry);
    }

}
