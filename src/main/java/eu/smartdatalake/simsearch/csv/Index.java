package eu.smartdatalake.simsearch.csv;


/**
 * Interface for indexing structure employed in similarity search (e.g., B+-tree, R-tree, etc.)
 * @param <K>  The keys for the indexed objects (e.g., datasetIdentifiers)
 * @param <V>  The values indexed in the structure (e.g., numerical values for B+-tree, geometries for R-tree, etc.)
 */
public interface Index<K,V> {

	//FIXME: Include abstract method(s) for building and accessing the various indices.

}
