package eu.smartdatalake.simsearch.pivoting.rtree;

import java.util.List;

import eu.smartdatalake.simsearch.pivoting.rtree.geometry.HasGeometry;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.ListPair;

public interface Splitter {

    /**
     * Splits a list of items into two lists of at least minSize.
     * 
     * @param <T>
     *            geometry type
     * @param items
     *            list of items to split
     * @param minSize
     *            min size of each list
     * @return two lists
     */
    <T extends HasGeometry> ListPair<T> split(List<T> items, int minSize);
}
