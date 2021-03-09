package eu.smartdatalake.simsearch.pivoting.rtree.internal.util;

import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

//import com.github.davidmoten.guavamini.Preconditions;

//import eu.smartdatalake.simsearch.pivoting.rtree.Node;
import eu.smartdatalake.simsearch.pivoting.rtree.Candidate;

public final class CandidatePriorityQueue<Candidate> {

    private final PriorityQueue<Candidate> queue; /* backing data structure */
    private final Comparator<Candidate> comparator;
    private int maxElements;   // Largest number of elements ever observed in the queue

    /**
     * Constructs a {@link CandidatePriorityQueue} with the specified {@code comparator}.
     *
     * @param comparator
     *            - The comparator to be used to compare the elements in the
     *            queue, must be non-null.
     */  
    public CandidatePriorityQueue(Comparator<Candidate> comparator) {
    	
//      Preconditions.checkNotNull(comparator, "comparator cannot be null");
        if (comparator == null) {
            throw new NullPointerException("comparator cannot be null");
        }
        
        this.queue = new PriorityQueue<Candidate>(comparator);
        this.comparator = comparator;
        this.maxElements = 0;
	}
 
  /*
	private Comparator<? super Candidate> reverse(Comparator<Candidate> comparator) {
        return new Comparator<Candidate>() {

            @Override
            public int compare(Candidate o1, Candidate o2) {
                return comparator.compare(o2, o1);
            }
        };
    }
	

	public CandidatePriorityQueue(Comparator<Candidate<Node<?, ?>, Double>> comparator) {
        Preconditions.checkNotNull(comparator, "comparator cannot be null");
        this.queue = new PriorityQueue<T>(reverse(comparator));
        this.comparator = comparator;
	}

	private static <T> Comparator<Candidate> reverse(final Comparator<T> comparator) {
        return new Comparator<T>() {

            @Override
            public int compare(T o1, T o2) {
                return comparator.compare(o2, o1);
            }
        };
    }
*/
    public static <Candidate> CandidatePriorityQueue<Candidate> create(final Comparator<Candidate> comparator) {
        return new CandidatePriorityQueue<Candidate>(comparator);
    }

    /**
     * Adds an element to the queue.
     *
     * @param t
     *            - Element to be added, must be non-null.
     */
    public void enqueue(final Candidate t) {
    	
        if (t == null) {
            throw new NullPointerException("Cannot add null to the queue");
        }
        
        queue.add(t);
        // Keep the max number of elements ever observed in the queue
        if (queue.size() > maxElements)
        	maxElements = queue.size();
//        System.out.println("Priority queue contains " + queue.size() + " elements");
    }

    /**
     * @return Returns a view of the queue as a
     *         {@link Collections#unmodifiableList(java.util.List)}
     *         unmodifiableList sorted in reverse order.
     */
    public List<Candidate> asList() {
        return Collections.unmodifiableList(new ArrayList<Candidate>(queue));
    }

    public List<Candidate> asOrderedList() {
        List<Candidate> list = new ArrayList<Candidate>(queue);
        Collections.sort(list, comparator);
        return list;
    }

    /**
     * Returns true if this priority queue contains no elements. 
     * @return
     */
    public boolean isEmpty() {
    	return queue.isEmpty();
    }
    
    /**
     * Retrieves and removes the head of this priority queue, or returns null if this priority queue is empty.
     * @return
     */
    public Candidate dequeue() {
    	return queue.poll();
    }
    
    /**
     * Retrieves, but does not remove, the head of this priority queue, or returns null if this priority queue is empty.
     * @return
     */
    public Candidate peek() {
    	return queue.peek();
    }
    
    public int getMaxElements() {
    	return maxElements;
    }
    
}
