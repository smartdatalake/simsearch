package eu.smartdatalake.simsearch.pivoting.rtree;

import static java.util.Collections.min;

import java.util.List;

import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Geometry;
import eu.smartdatalake.simsearch.pivoting.rtree.internal.Comparators;

/**
 * Uses minimal volume increase to select a node from a list.
 *
 */
public final class SelectorMinimalVolumeIncrease implements Selector {

    public static final SelectorMinimalVolumeIncrease INSTANCE = new SelectorMinimalVolumeIncrease();

    private SelectorMinimalVolumeIncrease() {
    }

    @Override
    public <T, S extends Geometry> Node<T, S> select(Geometry g, List<? extends Node<T, S>> nodes) {
        return min(nodes, Comparators.volumeIncreaseThenVolumeComparator(g.mbr()));
    }
}
