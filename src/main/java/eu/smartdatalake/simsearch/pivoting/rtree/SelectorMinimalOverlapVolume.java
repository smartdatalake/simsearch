package eu.smartdatalake.simsearch.pivoting.rtree;

import static java.util.Collections.min;

import java.util.List;

import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Geometry;
import eu.smartdatalake.simsearch.pivoting.rtree.internal.Comparators;

public final class SelectorMinimalOverlapVolume implements Selector {
    
    public static final SelectorMinimalOverlapVolume INSTANCE = new SelectorMinimalOverlapVolume();
    
    private SelectorMinimalOverlapVolume() {
    }

    @Override
    public <T, S extends Geometry> Node<T, S> select(Geometry g, List<? extends Node<T, S>> nodes) {
        return min(nodes,
                Comparators.overlapVolumeThenVolumeIncreaseThenVolumeComparator(g.mbr(), nodes));
    }

}
