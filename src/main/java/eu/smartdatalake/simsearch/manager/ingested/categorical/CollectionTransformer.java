package eu.smartdatalake.simsearch.manager.ingested.categorical;

import java.util.Arrays;

import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

public class CollectionTransformer {

	// Provides the document frequency of each token
	public TokenFrequencyPair[] calculateTokenFrequency(TokenSetCollection rawCollection) {

		// Compute token frequencies
		TObjectIntMap<String> tokenDict = new TObjectIntHashMap<String>();
		int frequency = 0;
		for (TokenSet set : rawCollection.sets.values()) {
			for (String token : set.tokens) {
				frequency = tokenDict.get(token);
				frequency++;
				tokenDict.put(token, frequency);
			}
		}

		// Sort tokens by frequency
		TokenFrequencyPair[] tfs = new TokenFrequencyPair[tokenDict.size()];
		TokenFrequencyPair tf;
		int counter = 0;
		for (String token : tokenDict.keySet()) {
			tf = new TokenFrequencyPair();
			tf.token = token;
			tf.frequency = tokenDict.get(token);
			tfs[counter] = tf;
			counter++;
		}
		Arrays.sort(tfs);

		return tfs;
	}

	// Provides the document frequency of each token
	public TokenFrequencyPair[] calculateTokenFrequency(IntSetCollection rawCollection) {

		// Compute token frequencies
		TObjectIntMap<String> tokenDict = new TObjectIntHashMap<String>();
		int frequency = 0;
		for (int[] set : rawCollection.sets) {
			for (int token : set) {
				frequency = tokenDict.get("" + token);
				frequency++;
				tokenDict.put("" + token, frequency);
			}
		}

		// Sort tokens by frequency
		TokenFrequencyPair[] tfs = new TokenFrequencyPair[tokenDict.size()];
		TokenFrequencyPair tf;
		int counter = 0;
		for (String token : tokenDict.keySet()) {
			tf = new TokenFrequencyPair();
			tf.token = token;
			tf.frequency = tokenDict.get(token);
			tfs[counter] = tf;
			counter++;
		}
		Arrays.sort(tfs);

		return tfs;
	}

	public TObjectIntMap<String> constructTokenDictionary(TokenSetCollection rawCollection) {

		// Sort tokens by frequency
		TokenFrequencyPair[] tfs = calculateTokenFrequency(rawCollection);

		// Assign integer IDs to tokens
		TObjectIntMap<String> tokenDict = new TObjectIntHashMap<String>();
		for (int i = 0; i < tfs.length; i++) {
			tokenDict.put(tfs[i].token, i);
		}

		return tokenDict;
	}

	public IntSetCollection transformCollection(TokenSetCollection rawCollection, TObjectIntMap<String> tokenDict) {

		// Transform each raw set
		TokenSet[] rsets = rawCollection.sets.values().toArray(new TokenSet[0]);
		IntSet[] tsets = new IntSet[rsets.length];
		IntSet tset;
		TokenSet rset;
		String[] rtokens;
		TObjectIntMap<String> unknownTokenDict = new TObjectIntHashMap<String>();
		for (int i = 0; i < rsets.length; i++) {
			rset = rsets[i];
			rtokens = rset.tokens.toArray(new String[0]);
			tset = new IntSet();
			tset.id = rset.id;
			tset.originalString = rset.originalString;

			// map string tokens to ints
			tset.tokens = new int[rtokens.length];
			for (int j = 0; j < rtokens.length; j++) {
				if (tokenDict.containsKey(rtokens[j])) {
					tset.tokens[j] = tokenDict.get(rtokens[j]);
				} else if (unknownTokenDict.containsKey(rtokens[j])) {
					tset.tokens[j] = unknownTokenDict.get(rtokens[j]);
				} else {
					tset.tokens[j] = -1 * (unknownTokenDict.size() + 1);
					unknownTokenDict.put(rtokens[j], tset.tokens[j]);
				}
			}

			// sort int tokens
			Arrays.sort(tset.tokens);

			tsets[i] = tset;
		}

		// Populate the collection
		IntSetCollection collection = new IntSetCollection();

		// Sort sets by their length and tokens

//		List<Integer> tlengths = new ArrayList<Integer>();
//		for (int i=0; i<tsets.length; i++)
//			tlengths.add(tsets[i].tokens.length);
//		List<String> sortedList = new ArrayList<String>(Arrays.asList(rawCollection.getOriginalStrings()));
//		sortedList.sort(Comparator.comparingInt(tlengths::indexOf));
//		collection.originalStrings = Arrays.stream(sortedList.toArray()).toArray(String[]::new);
		collection.originalStrings = rawCollection.getOriginalStrings();
		
		Arrays.sort(tsets);

		collection.numTokens = tokenDict.size();
		collection.sets = new int[tsets.length][];
		collection.keys = new String[tsets.length];
//		collection.idMap = new TIntObjectHashMap<>();
		for (int i = 0; i < collection.sets.length; i++) {
			collection.sets[i] = tsets[i].tokens;
//			collection.idMap.put(i, tsets[i].id);
			collection.keys[i] = tsets[i].id;
		}
		return collection;
	}

	public class TokenFrequencyPair implements Comparable<TokenFrequencyPair> {
		String token;
		int frequency;

		public String getToken() {
			return this.token;
		}

		public int getFrequency() {
			return this.frequency;
		}

		@Override
		public int compareTo(TokenFrequencyPair tf) {
			int r = this.frequency == tf.frequency ? this.token.compareTo(tf.token) : this.frequency - tf.frequency;
			return r;
		}

		@Override
		public String toString() {
			return token + "->" + frequency;
		}
	}

}