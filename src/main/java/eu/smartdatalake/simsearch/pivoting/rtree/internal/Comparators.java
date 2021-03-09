package eu.smartdatalake.simsearch.pivoting.rtree.internal;

import java.util.Comparator;
import java.util.List;

import eu.smartdatalake.simsearch.pivoting.rtree.Candidate;
import eu.smartdatalake.simsearch.pivoting.rtree.Entry;
import eu.smartdatalake.simsearch.pivoting.rtree.Selector;
import eu.smartdatalake.simsearch.pivoting.rtree.Splitter;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Geometry;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.HasGeometry;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Rectangle;

/**
 * Utility functions asociated with {@link Comparator}s, especially for use with
 * {@link Selector}s and {@link Splitter}s.
 * @param <R>
 * @param <D>
 * 
 */
public final class Comparators<R, D> {

    private Comparators() {
        // prevent instantiation
    }

    public static <T extends HasGeometry> Comparator<HasGeometry> overlapVolumeThenVolumeIncreaseThenVolumeComparator(
            final Rectangle r, final List<T> list) {
        return new Comparator<HasGeometry>() {

            @Override
            public int compare(HasGeometry g1, HasGeometry g2) {
                int value = Double.compare(overlapVolume(r, list, g1), overlapVolume(r, list, g2));
                if (value == 0) {
                    value = Double.compare(volumeIncrease(r, g1), volumeIncrease(r, g2));
                    if (value == 0) {
                        value = Double.compare(volume(r, g1), volume(r, g2));
                    }
                }
                return value;
            }
        };
    }

    private static double volume(final Rectangle r, HasGeometry g1) {
        return g1.geometry().mbr().add(r).volume();
    }

    public static <T extends HasGeometry> Comparator<HasGeometry> volumeIncreaseThenVolumeComparator(
            final Rectangle r) {
        return new Comparator<HasGeometry>() {
            @Override
            public int compare(HasGeometry g1, HasGeometry g2) {
                int value = Double.compare(volumeIncrease(r, g1), volumeIncrease(r, g2));
                if (value == 0) {
                    value = Double.compare(volume(r, g1), volume(r, g2));
                }
                return value;
            }
        };
    }

    private static float overlapVolume(Rectangle r, List<? extends HasGeometry> list, HasGeometry g) {
        Rectangle gPlusR = g.geometry().mbr().add(r);
        float m = 0;
        for (HasGeometry other : list) {
            if (other != g) {
                m += gPlusR.intersectionVolume(other.geometry().mbr());
            }
        }
        return m;
    }

    private static double volumeIncrease(Rectangle r, HasGeometry g) {
        Rectangle gPlusR = g.geometry().mbr().add(r);
        return gPlusR.volume() - g.geometry().mbr().volume();
    }

    /**
     * <p>
     * Returns a comparator that can be used to sort entries returned by search
     * methods. For example:
     * </p>
     * <p>
     * <code>search(100).toSortedList(ascendingDistance(r))</code>
     * </p>
     * 
     * @param <T>
     *            the value type
     * @param <S>
     *            the entry type
     * @param r
     *            rectangle to measure distance to
     * @return a comparator to sort by ascending distance from the rectangle
     */
    public static <T, S extends Geometry> Comparator<Entry<T, S>> ascendingDistance(
            final Rectangle r) {
        return new Comparator<Entry<T, S>>() {
            @Override
            public int compare(Entry<T, S> e1, Entry<T, S> e2) {
                return Double.compare(e1.geometry().distance(r), e2.geometry().distance(r));
            }
        };
    }

/*
    public static <R extends Node<?, ?>, D> Comparator<NodeCandidate<Node<?, ?>, Double>> increasingDistance() {
        return new Comparator<NodeCandidate<Node<?, ?>, Double>>() {
			public int compare(NodeCandidate<Node<?, ?>, Double> o1, NodeCandidate<Node<?, ?>, Double> o2) {
				// TODO Auto-generated method stub
				return Double.compare(o1.distance(), o2.distance());
			}
        };
    }
*/  
    /**
     * Returns a comparator that can be used to sort elements returned by the distance browsing method that searches for k-nearest neighbors.
     * 
     * @param <R>
     *            the element type (node or entry)
     * @param <D>
     *            the distance type
     * @return  a comparator to sort elements in the priority queue by ascending distance when searching for k-nearest neighbors
     */
    public static <R, D> Comparator<Candidate<Object, Double>> ascendingDistance() {
        return new Comparator<Candidate<Object, Double>>() {
			public int compare(Candidate<Object, Double> o1, Candidate<Object, Double> o2) {
				return Double.compare(o1.distance(), o2.distance());
			}
        };
    }
    
    /**
     * Returns a comparator that can be used to sort entries returned by the distance browsing method that searches for k-nearest neighbors.
     * 
     * @param <T>
     *            the value type
     * @param <S>
     *            the entry type
     * @param <D>
     *            the distance type
     * @return  a comparator to sort by ascending distance when searching for k-nearest neighbors
     */
/*    
    public static <T, S extends Geometry, D extends Double> Comparator<NearestEntry<T, S, D>> ascendingDistance() {
        return new Comparator<NearestEntry<T, S, D>>() {
            @Override
            public int compare(NearestEntry<T, S, D> e1, NearestEntry<T, S, D> e2) {
                return Double.compare(e1.distance(), e2.distance());
            }
        };
    }
*/    


  
}
