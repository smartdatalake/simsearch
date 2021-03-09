package eu.smartdatalake.simsearch.pivoting.rtree;

//import com.github.davidmoten.guavamini.Preconditions;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Geometry;

/**
 * Configures an RTree prior to instantiation of an {@link RTree}.
 * @param <T> value type
 * @param <S> geometry type
 */
public final class Context<T, S extends Geometry> {

    private final int dimensions;
    private final int maxChildren;
    private final int minChildren;
    private final Splitter splitter;
    private final Selector selector;
    private final Factory<T, S> factory;

    /**
     * Constructor.
     * 
     * @param minChildren
     *            minimum number of children per node (at least 1)
     * @param maxChildren
     *            max number of children per node (minimum is 3)
     * @param selector
     *            algorithm to select search path
     * @param splitter
     *            algorithm to split the children across two new nodes
     * @param factory
     *            node creation factory
     */
    public Context(int dimensions, int minChildren, int maxChildren, Selector selector, Splitter splitter,
            Factory<T, S> factory) {
/*    	
        Preconditions.checkNotNull(splitter);
        Preconditions.checkNotNull(selector);
        Preconditions.checkArgument(maxChildren > 2, "maxChildren must be greater than 2");
        Preconditions.checkArgument(minChildren >= 1, "minChildren must be greater than 0");
        Preconditions.checkArgument(minChildren < maxChildren, "minChildren must be less than maxChildren");
        Preconditions.checkNotNull(factory);
        Preconditions.checkArgument(dimensions > 1, "dimensions must be greater than 1");
*/ 	
    	if (splitter == null)
    		throw new NullPointerException();
    	if (selector == null)
    		throw new NullPointerException();
    	if (maxChildren <= 2)
            throw new IllegalArgumentException("maxChildren must be greater than 2");
    	if (minChildren < 1)
            throw new IllegalArgumentException("minChildren must be greater than 0");
    	if (minChildren >= maxChildren)
            throw new IllegalArgumentException("minChildren must be less than maxChildren");
    	if (factory == null)
    		throw new NullPointerException();
    	if (dimensions <= 1)
            throw new IllegalArgumentException("dimensions must be greater than 1");

        this.dimensions = dimensions;
        this.selector = selector;
        this.maxChildren = maxChildren;
        this.minChildren = minChildren;
        this.splitter = splitter;
        this.factory = factory;
    }

    public int maxChildren() {
        return maxChildren;
    }

    public int minChildren() {
        return minChildren;
    }

    public Splitter splitter() {
        return splitter;
    }

    public Selector selector() {
        return selector;
    }

    public Factory<T, S> factory() {
        return factory;
    }

    public int dimensions() {
        return dimensions;
    }

}
