package eu.smartdatalake.simsearch.pivoting.rtree.internal;

//import com.github.davidmoten.guavamini.Objects;
//import com.github.davidmoten.guavamini.Preconditions;
import eu.smartdatalake.simsearch.pivoting.rtree.NearestEntry;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Geometry;
import eu.smartdatalake.simsearch.pivoting.rtree.internal.util.ObjectsHelper;

/**
 * An entry in the R-tree which has a spatial representation along with its distance from a given query point.
 * 
 * @param <T>
 *            value type
 * @param <S>
 *            geometry type
 * @param <D>
 *            distance type
 * 		
 */
public final class  NearestEntryDefault<T, S extends Geometry, D> implements NearestEntry<T, S, D> {
    private final T value;
    private final S geometry;
    private final D distance;


    /**
     * Constructor.
     * 
     * @param value
     *            the value of the entry
     * @param geometry
     *            the geometry of the value
     * @param distance
     *            the distance from the query point
     */
    public  NearestEntryDefault(T value, S geometry, D distance) {
    	
 //     Preconditions.checkNotNull(geometry);
    	if (geometry == null)
            throw new NullPointerException();
    	
        this.value = value;
        this.geometry = geometry;
        this.distance = distance;
    }

    /**
     * Factory method.
     * 
     * @param <T>
     *            type of value
     * @param <S>
     *            type of geometry
     * @param value
     *            object being given a spatial context
     * @param geometry
     *            geometry associated with the value
     * @param distance
     *            distance of this geometry from the query point
     * @return entry wrapping value, associated geometry, and distance from the query point
     */
    public static <T, S extends Geometry, D> NearestEntry<T, S, D> nearestEntry(T value, S geometry, D distance) {
        return new  NearestEntryDefault<T, S, D>(value, geometry, distance);
    }

    /**
     * Returns the value wrapped by this {@link  NearestEntryDefault}.
     * 
     * @return the entry value
     */
    @Override
    public T value() {
        return value;
    }

    @Override
    public S geometry() {
        return geometry;
    }

    @Override
    public D distance() {
        return distance;
    }
    
    @Override
    public String toString() {
        String builder = "Entry [value=" + value + ", geometry=" + geometry + ", distance=" + distance + "]";
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value, geometry);
    }

    @Override
    public boolean equals(Object obj) {
        @SuppressWarnings("rawtypes")
         NearestEntryDefault other = ObjectsHelper.asClass(obj,  NearestEntryDefault.class);
        if (other != null) {
            return Objects.equal(value, other.value)
                    && Objects.equal(geometry, other.geometry)
                    && Objects.equal(distance, other.distance);
        } else
            return false;
    }

}

