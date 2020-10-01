package eu.smartdatalake.simsearch.csv.numerical;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import eu.smartdatalake.simsearch.csv.Index;


/**
 * Implements a B+-tree index to be used in similarity search against numerical values.
 * This source code adjusts and extends the one available at https://github.com/jiaguofang/b-plus-tree
 * TODO: Implement serialization of the constructed index.
 * @param <K>  Type variable representing the keys of the indexed objects.
 * @param <V>  Type variable representing the values of the indexed objects.
 */
public class BPlusTree<K extends Comparable<? super K>, V> implements Index<Object, Object>, Serializable {

	private static final long serialVersionUID = 1L;

	public static enum RangePolicy {
		EXCLUSIVE, INCLUSIVE
	}

	/**
	 * The branching factor used when none specified in constructor.
	 */
	private static final int DEFAULT_BRANCHING_FACTOR = 128;

	/**
	 * The branching factor for the B+ tree, that measures the capacity of nodes
	 * (i.e., the number of children nodes) for internal nodes in the tree.
	 */
	private int branchingFactor;

	/**
	 * The root node of the B+ tree.
	 */
	private Node root;

	public int numNodes;
	public int numLeaves;


	/**
	 * Constructor #1 without specifying branching factor per node
	 */
	public BPlusTree() {
		this(DEFAULT_BRANCHING_FACTOR);
		numNodes = numLeaves = 0;
	}

	/**
	 * Constructor #2
	 * 
	 * @param branchingFactor
	 *            An integer indicating the branching factor per node
	 */
	public BPlusTree(int branchingFactor) {
		if (branchingFactor <= 2)
			throw new IllegalArgumentException("Illegal branching factor: " + branchingFactor);
		this.branchingFactor = branchingFactor;
		numNodes = numLeaves = 0;
		root = new LeafNode();
	}

	/**
	 * FIXME: Serialize the built index into a file; NOT working yet!
	 * @param filename  Path to the file on disk
	 */
	public void serialize(String filename) {

		try {
			// Save the entire tree as an object into a file

			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutput out = new ObjectOutputStream(bos);
			out.writeObject(this);
			out.flush();
			byte[] treeBytes = bos.toByteArray();

			FileOutputStream file = new FileOutputStream(filename);
			ObjectOutputStream outBytes = new ObjectOutputStream(file);
			outBytes.writeObject(treeBytes);

			outBytes.flush();
			out.close();
			bos.close();
			file.close();

			System.out.println("Index has been serialized");

		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * Returns the value to which the specified key is associated, or
	 * {@code null} if this tree contains no association for the key.
	 *
	 * <p>
	 * A return value of {@code null} does not <i>necessarily</i> indicate that
	 * the tree contains no association for the key; it's also possible that the
	 * tree explicitly associates the key to {@code null}.
	 * 
	 * @param key
	 *            the key whose associated value is to be returned
	 * 
	 * @return the value to which the specified key is associated, or
	 *         {@code null} if this tree contains no association for the key
	 */
	public List<V> search(K key) {
		return root.getValue(key);
	}

	/**
	 * A handle to the root of the tree.
	 * @return  The root node.
	 */
	public Node getRoot() {
		return root;
	}
	
	
	/**
	 * Auxiliary method to calculate the minimum key currently indexed in the B+-tree.
	 * This is utilized to provide the range of indexed values when estimating similarity scores.
	 * @return  The minimum key.
	 */
	public K calcMinKey() {
		return root.getFirstLeafKey();
	}

	/**
	 * Auxiliary method to calculate the maximum key currently indexed in the B+-tree.
	 * This is utilized to provide the range of indexed values when estimating similarity scores.
	 * @return  The maximum key.
	 */
	public K calcMaxKey() {
		return root.getLastLeafKey();
	}


	/**
	 * Associates the specified value with the specified key in this tree. If
	 * the tree previously contained a association for the key, the old value is
	 * replaced.
	 * 
	 * @param key
	 *            the key with which the specified value is to be associated
	 * @param value
	 *            the value to be associated with the specified key
	 */
	public void insert(K key, V value) {
		root.insertValue(key, value);
	}

	/**
	 * Removes the association for the specified key from this tree if present.
	 * 
	 * @param key
	 *            the key whose association is to be removed from the tree
	 */
	public void delete(K key) {
		root.deleteValue(key);
	}

	/**
	 * Prints out of the index keys level by level (from root to leaves)
	 */
	@SuppressWarnings("unchecked")
	public String toString() {
		Queue<List<Node>> queue = new LinkedList<List<Node>>();
		queue.add(Arrays.asList(root));
		StringBuilder sb = new StringBuilder();
		while (!queue.isEmpty()) {
			Queue<List<Node>> nextQueue = new LinkedList<List<Node>>();
			while (!queue.isEmpty()) {
				List<Node> nodes = queue.remove();
				sb.append('{');
				Iterator<Node> it = nodes.iterator();
				while (it.hasNext()) {
					Node node = it.next();
					sb.append(node.hashCode() + "->" + node.toString());
					if (it.hasNext())
						sb.append(", ");
					if (node instanceof BPlusTree.InternalNode)
						nextQueue.add(((InternalNode) node).children);
				}
				sb.append('}');
				if (!queue.isEmpty())
					sb.append(", ");
				else
					sb.append('\n');
			}
			queue = nextQueue;
		}

		return sb.toString();
	}

	/**
	 * Generic Node class.
	 */
	abstract class Node implements Serializable {

		private static final long serialVersionUID = 1L;

		List<K> keys;

		int keyNumber() {
			return keys.size();
		}

		abstract List<V> getValue(K key);

		abstract void deleteValue(K key);

		abstract void insertValue(K key, V value);

		abstract K getFirstLeafKey();

		abstract K getLastLeafKey();

		// IMPORTANT! Since Java does NOT generally support call by reference, a
		// pair of visited leaves is passed as a collection in order to be able
		// to make changes during traversal
		abstract List<V> getRange(List<LeafNode> pairLeaves, K key1, RangePolicy policy1, K key2, RangePolicy policy2);

		abstract void merge(Node sibling);

		abstract Node split();

		abstract boolean isOverflow();

		abstract boolean isUnderflow();

		public String toString() {
			return keys.toString();
		}

		public abstract K nextKeyLeftwards(K key);

		public abstract K nextKeyRightwards(K rightKey);

	}

	/**
	 * Nested class used for representation of internal nodes.
	 */
	private class InternalNode extends Node implements Serializable {

		private static final long serialVersionUID = 1L;

		List<Node> children;

		InternalNode() {
			this.keys = new ArrayList<K>();
			this.children = new ArrayList<Node>();
			numNodes++;
		}

		@Override
		List<V> getValue(K key) {
			return getChild(key).getValue(key);
		}

		public K nextKeyLeftwards(K key) {
			Node child = getChild(key);
			K leftKey = child.nextKeyLeftwards(key);

			if (leftKey == null) {
				child = getChildLeftSibling(child.getFirstLeafKey());
				if (child != null) {
					leftKey = child.getLastLeafKey();
				}
			}

			return leftKey;
		}

		public K nextKeyRightwards(K key) {

			Node child = getChild(key);
			K rightKey = child.nextKeyRightwards(key);
			if (rightKey == null) {
				child = getChildRightSibling(child.getLastLeafKey());
				if (child != null)
					rightKey = child.getFirstLeafKey();
			}
			return rightKey;
		}

		@Override
		void deleteValue(K key) {
			Node child = getChild(key);
			child.deleteValue(key);
			if (child.isUnderflow()) {
				Node childLeftSibling = getChildLeftSibling(key);
				Node childRightSibling = getChildRightSibling(key);
				Node left = childLeftSibling != null ? childLeftSibling : child;
				Node right = childLeftSibling != null ? child : childRightSibling;
				left.merge(right);
				deleteChild(right.getFirstLeafKey());
				if (left.isOverflow()) {
					Node sibling = left.split();
					insertChild(sibling.getFirstLeafKey(), sibling);
				}
				if (root.keyNumber() == 0)
					root = left;
			}
		}

		@Override
		void insertValue(K key, V value) {
			Node child = getChild(key);
			child.insertValue(key, value);
			if (child.isOverflow()) {
				Node sibling = child.split();
				insertChild(sibling.getFirstLeafKey(), sibling);
			}
			if (root.isOverflow()) {
				Node sibling = split();
				InternalNode newRoot = new InternalNode();
				newRoot.keys.add(sibling.getFirstLeafKey());
				newRoot.children.add(this);
				newRoot.children.add(sibling);
				root = newRoot;
			}
		}

		@Override
		K getFirstLeafKey() {
			return children.get(0).getFirstLeafKey();
		}

		@Override
		K getLastLeafKey() {
			return children.get(children.size() - 1).getLastLeafKey();
		}

		@Override
		List<V> getRange(List<LeafNode> pairLeaves, K key1, RangePolicy policy1, K key2, RangePolicy policy2) {
			return getChild(key1).getRange(pairLeaves, key1, policy1, key2, policy2);
		}

		@Override
		void merge(Node sibling) {
			@SuppressWarnings("unchecked")
			InternalNode node = (InternalNode) sibling;
			keys.add(node.getFirstLeafKey());
			keys.addAll(node.keys);
			children.addAll(node.children);
			--numNodes;

		}

		@Override
		Node split() {
			int from = keyNumber() / 2 + 1, to = keyNumber();
			InternalNode sibling = new InternalNode();
			sibling.keys.addAll(keys.subList(from, to));
			sibling.children.addAll(children.subList(from, to + 1));

			keys.subList(from - 1, to).clear();
			children.subList(from, to + 1).clear();

			return sibling;
		}

		@Override
		boolean isOverflow() {
			return children.size() > branchingFactor;
		}

		@Override
		boolean isUnderflow() {
			return children.size() < (branchingFactor + 1) / 2;
		}

		Node getChild(K key) {
			int loc = Collections.binarySearch(keys, key);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			return children.get(childIndex);
		}

		void deleteChild(K key) {
			int loc = Collections.binarySearch(keys, key);
			if (loc >= 0) {
				keys.remove(loc);
				children.remove(loc + 1);
			}
		}

		void insertChild(K key, Node child) {
			int loc = Collections.binarySearch(keys, key);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			if (loc >= 0) {
				children.set(childIndex, child);
			} else {
				keys.add(childIndex, key);
				children.add(childIndex + 1, child);
			}
		}

		Node getChildLeftSibling(K key) {
			int loc = Collections.binarySearch(keys, key);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			if (childIndex > 0)
				return children.get(childIndex - 1);

			return null;
		}

		Node getChildRightSibling(K key) {
			int loc = Collections.binarySearch(keys, key);
			int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
			if (childIndex < keyNumber())
				return children.get(childIndex + 1);

			return null;
		}
	}

	/**
	 * Nested class used for representation of leaves.
	 */
	private class LeafNode extends Node implements Serializable {

		private static final long serialVersionUID = 1L;

		List<List<V>> values; // Since keys must be unique, use this list to
								// keep any elements with duplicate keys
		LeafNode next;

		// Constructor of an empty new leaf
		LeafNode() {
			keys = new ArrayList<K>();
			values = new ArrayList<List<V>>();
			next = null;
			numLeaves++;
		}

		@Override
		List<V> getValue(K key) {
			int loc = Collections.binarySearch(keys, key);
			return loc >= 0 ? values.get(loc) : null;
		}

		public K nextKeyLeftwards(K key) {

			int loc = Collections.binarySearch(keys, key);

			if (loc > 0) { // Exact key match, so get the previous key
				return keys.get(loc - 1);
			} else if ((loc == 0) || (loc == -1)) { // Before the key range of
													// this node (or among all
													// nodes if the leftmost
													// leaf is reached)
				return null;
			} else { // Inside the key range or after the greatest key in this
						// leaf, so get the previous key
				return keys.get(-loc - 2);
			}

		}

		public K nextKeyRightwards(K key) {

			int loc = Collections.binarySearch(keys, key);

			if ((loc == keys.size() - 1) || (-loc == keys.size() + 1)) { 
				return null;  // Last item or beyond the key range
			} else if (loc >= 0) { // Exact key match
				return keys.get(loc + 1);
			} else { // Inside the key range, get the next key
				return keys.get(-loc - 1);
			}

		}

		@Override
		void deleteValue(K key) {
			int loc = Collections.binarySearch(keys, key);
			if (loc >= 0) {
				keys.remove(loc);
				values.remove(loc);
			}
		}

		@Override
		void insertValue(K key, V value) {
			int loc = Collections.binarySearch(keys, key);
			int valueIndex = loc >= 0 ? loc : -loc - 1;
			if (loc >= 0) { // Key exists,...
				values.get(valueIndex).add(value); // ..., so append this extra
													// value to the list
			} else {
				keys.add(valueIndex, key); // Key does not exist,...
				List<V> v = new ArrayList<V>(); // ..., so create a new list...
				v.add(value); // ... with this value as its first item
				values.add(valueIndex, v);
			}
			if (root.isOverflow()) {
				Node sibling = split();
				InternalNode newRoot = new InternalNode();
				newRoot.keys.add(sibling.getFirstLeafKey());
				newRoot.children.add(this);
				newRoot.children.add(sibling);
				root = newRoot;
			}
		}

		@Override
		K getFirstLeafKey() {
			return keys.get(0);
		}

		@Override
		K getLastLeafKey() {
			return keys.get(keys.size() - 1);
		}

		@Override
		List<V> getRange(List<LeafNode> pairLeaves, K key1, RangePolicy policy1, K key2, RangePolicy policy2) {
			List<V> result = new LinkedList<V>();

			// This is the first leaf in the range
			if ((pairLeaves == null) || (pairLeaves.get(1) == null)) {
				pairLeaves.set(1, this); // This is marked as the leaf to visit
											// next
			}

			// If there is any leaf to visit next
			if (pairLeaves.get(1) != null) {
				// Remember this leaf as currently visited
				pairLeaves.set(0, this);
				Iterator<K> kIt = pairLeaves.get(1).keys.iterator();
				Iterator<List<V>> vIt = pairLeaves.get(1).values.iterator();
				while (kIt.hasNext()) {
					K key = kIt.next();
					List<V> values = vIt.next();
					int cmp1 = key.compareTo(key1);
					int cmp2 = key.compareTo(key2);
					if (((policy1 == RangePolicy.EXCLUSIVE && cmp1 > 0)
							|| (policy1 == RangePolicy.INCLUSIVE && cmp1 >= 0))
							&& ((policy2 == RangePolicy.EXCLUSIVE && cmp2 < 0)
									|| (policy2 == RangePolicy.INCLUSIVE && cmp2 <= 0)))
						result.addAll(values);
					else if ((policy2 == RangePolicy.EXCLUSIVE && cmp2 >= 0)
							|| (policy2 == RangePolicy.INCLUSIVE && cmp2 > 0)) {
						return result;
					}
				}
				// Remember the leaf to visit next
				pairLeaves.set(1, this.next);

			}
			return result;
		}

		@Override
		void merge(Node sibling) {
			@SuppressWarnings("unchecked")
			LeafNode node = (LeafNode) sibling;
			keys.addAll(node.keys);
			values.addAll(node.values);
			next = node.next;
			--numLeaves;
		}

		@Override
		Node split() {
			LeafNode sibling = new LeafNode();
			int from = (keyNumber() + 1) / 2, to = keyNumber();
			sibling.keys.addAll(keys.subList(from, to));
			sibling.values.addAll(values.subList(from, to));

			keys.subList(from, to).clear();
			values.subList(from, to).clear();

			sibling.next = next;
			next = sibling;
			return sibling;
		}

		@Override
		boolean isOverflow() {
			return values.size() > branchingFactor - 1;
		}

		@Override
		boolean isUnderflow() {
			return values.size() < branchingFactor / 2;
		}
	}

}
