package eu.smartdatalake.simsearch.jdbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.postgresql.util.PGobject;

import eu.smartdatalake.simsearch.IValueFinder;
import eu.smartdatalake.simsearch.csv.categorical.TokenSet;

/**
 * Updates the in-memory lookup with CATEGORICAL values used in random access operations during rank aggregation.
 * This method inserts the categorical (keyword set) attribute value found for a given object identifier (primary key) in a database table.  
 * @param <K>  The key (identifier) of the object.
 * @param <V>  The textual value (keywords) of this object at a particular attribute.
 */
public class CategoricalValueFinder<K,V> implements IValueFinder<K,V> {

	String sqlTemplate = null;
	JdbcConnector databaseConnector = null;
	
	public CategoricalValueFinder(JdbcConnector jdbcConnector, String sql) {
		
		this.databaseConnector = jdbcConnector;
		this.sqlTemplate = sql;
	}
	
	@Override
	public TokenSet find(HashMap<K,V> dataset, K k) {

//		System.out.println("VALUE RETRIEVAL QUERY: " + sqlTemplate);
		
		Object val = databaseConnector.findSingletonValue(sqlTemplate.replace("$id", k.toString()));
		
		if (val instanceof PGobject) {
			// FIXME: Specific for PostgreSQL: Expunge [ and ] from the returned array, as well as double quotes
			String keywords = val.toString().replace("\"", "");
			keywords = keywords.substring(1, keywords.length()-1);
			TokenSet tokenset = tokenize(k.toString(), keywords, ",");   // FIXME: comma is the delimiter in PostgreSQL arrays
			dataset.put(k, (V)tokenset);
			return tokenset;
		}
		return null;
	}

	/**
	 * Tokenizes the given string of keywords using a specific character as delimiter.
	 * @param id   Identifier of this collection of keywords.
	 * @param keywords   A string containing keywords.
	 * @param tokDelimiter   The delimiter character between keywords.
	 * @return  A set of tokens (keywords).
	 */
	public TokenSet tokenize(String id, String keywords, String tokDelimiter) {
		
		TokenSet set = null;
		try {
			if (keywords != null) {
				set = new TokenSet();
				set.id = id; 
				set.tokens = new ArrayList<String>();
				// Split and trim keywords
				List<String> tokens = new ArrayList<String>(new HashSet<String>(Arrays.asList(keywords.split("\\s*" + tokDelimiter + "\\s*"))));
				set.tokens.addAll(tokens);
//				tokens.forEach(System.out::println);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return set;
	}
}
