package eu.smartdatalake.simsearch.csv.categorical;


import eu.smartdatalake.simsearch.csv.Index;
import gnu.trove.list.TIntList;
import gnu.trove.map.TObjectIntMap;

public class InvertedIndex implements Index<Object, Object> {

	public TIntList[] idx;                 //Handle to the underlying inverted index (for categorical keyword search)

	public IntSetCollection transformedTargetCollection;
	
	public TObjectIntMap<String> tokenDictionary;

	public IntSetCollection getTransformedCollection(TokenSetCollection queryCollection) {
		
		CollectionTransformer transformer = new CollectionTransformer();
		return transformer.transformCollection(queryCollection, this.tokenDictionary);
	}
}
