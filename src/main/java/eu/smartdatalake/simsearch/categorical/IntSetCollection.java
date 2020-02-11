package eu.smartdatalake.simsearch.categorical;

import java.util.Arrays;

public class IntSetCollection {
//	public TIntObjectMap<String> idMap;
	public int numTokens;
	public int[][] sets;
	public String[] originalStrings;
	public String[] keys;
	
	@Override
	public String toString() {
		return Arrays.deepToString(sets) + " "+ Arrays.toString(keys);
	}
}