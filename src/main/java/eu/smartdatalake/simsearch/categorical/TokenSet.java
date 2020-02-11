package eu.smartdatalake.simsearch.categorical;

import java.util.List;

public class TokenSet {
	public String id;
	public List<String> tokens;
	public String originalString;

	@Override
	public String toString() {
//		return id + ": " + tokens.toString();
		return tokens.toString();   //Avoids printing the identifier
	}

}