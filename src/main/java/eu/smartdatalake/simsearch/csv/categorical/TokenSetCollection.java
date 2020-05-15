package eu.smartdatalake.simsearch.csv.categorical;

import java.util.HashMap;
import java.util.Map;

public class TokenSetCollection {
//	public TokenSet[] sets;
	
	public HashMap<String, TokenSet> sets;
	
	public TokenSetCollection() {
		sets = new HashMap<String, TokenSet>();
	}
	
	public String[] getOriginalStrings() {
		String[] temp = new String[sets.size()];
		int i = 0;
		for (Map.Entry<String, TokenSet> entry : sets.entrySet()) {
			temp[i] = entry.getValue().originalString;
			i++;
		}
		return temp;
	}
}