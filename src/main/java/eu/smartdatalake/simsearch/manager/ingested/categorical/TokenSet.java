package eu.smartdatalake.simsearch.manager.ingested.categorical;

import java.util.List;

public class TokenSet {
	public String id;
	public List<String> tokens;
	public String originalString;

	@Override
	public String toString() {
//		return id + ": " + tokens.toString();
		//Avoid printing the identifier; in case of q-grams, return the original string
		return ((originalString != null) ? originalString : tokens.toString());   
	}

}