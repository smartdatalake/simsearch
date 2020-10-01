package eu.smartdatalake.simsearch.ranking.randomaccess;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.postgresql.util.PGobject;

import eu.smartdatalake.simsearch.IDataConnector;
import eu.smartdatalake.simsearch.IValueFinder;
import eu.smartdatalake.simsearch.csv.categorical.TokenSet;

/**
 * Updates the in-memory lookup with CATEGORICAL values used in random access operations during rank aggregation.
 * This method inserts the categorical (keyword set) attribute value found for a given object identifier (primary key) in a data source collection (DBMS table of REST API data).
 * @param <K>  The key (identifier) of the object.
 * @param <V>  The textual value (keywords) of this object at a particular attribute.
 */
public class CategoricalValueFinder<K,V> implements IValueFinder<K,V> {

	String queryTemplate = null;
	IDataConnector dataConnector = null;
	
	/**
	 * Constructor of this class
	 * @param dataConnector  Instance of a connector to data source (JDBC connection to a DBMS or HTTP connection to a REST API).
	 * @param query  The query template to execute, either in SQL (against a DBMS) or JSON (against a REST API).
	 */
	public CategoricalValueFinder(IDataConnector dataConnector, String query) {
		
		this.dataConnector = dataConnector;
		this.queryTemplate = query;
	}
	
	@Override
	public TokenSet find(HashMap<K,V> dataset, K k) {

//		System.out.println("VALUE RETRIEVAL QUERY: " + sqlTemplate);
		
		// Replace identifier in the query template and execute
		Object val = dataConnector.findSingletonValue(queryTemplate.replace("$id", k.toString()));

		// Process the single value to return a token set
		if (val != null) {
			// Eliminate double quotes
			String keywords = val.toString().replace("\"", "");
			//System.out.println("CATEGORICAL VALUE: " + keywords);
		
			// FIXME: Specific for PostgreSQL: Expunge [ and ] from the returned array
			if (val instanceof PGobject) {
				keywords = keywords.substring(1, keywords.length()-1);
			}
			// FIXME: comma is the default delimiter in PostgreSQL arrays and in Elasticsearch; only handling this case only
			TokenSet tokenset = tokenize(k.toString(), keywords, ",");  
			
			// Insert tokenset into the lookup
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
