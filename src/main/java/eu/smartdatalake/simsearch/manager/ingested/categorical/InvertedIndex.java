package eu.smartdatalake.simsearch.manager.ingested.categorical;

import eu.smartdatalake.simsearch.manager.ingested.Index;
import gnu.trove.list.TIntList;
import gnu.trove.map.TObjectIntMap;

/** 
 * Wrapper of the inverted index facilities for categorical (set-based) similarity search.
 */
public class InvertedIndex implements Index<Object, Object> {

	public TIntList[] idx;                 //Handle to the underlying inverted index (for categorical keyword search)

	public IntSetCollection transformedTargetCollection;
	
	public TObjectIntMap<String> tokenDictionary;

	public IntSetCollection getTransformedCollection(TokenSetCollection queryCollection) {
		
		CollectionTransformer transformer = new CollectionTransformer();
		return transformer.transformCollection(queryCollection, this.tokenDictionary);
	}
}
