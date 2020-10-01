package eu.smartdatalake.simsearch.csv.categorical;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import eu.smartdatalake.simsearch.Logger;
import eu.smartdatalake.simsearch.csv.DataFileReader;
import eu.smartdatalake.simsearch.jdbc.JdbcConnector;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TObjectIntMap;

public class TokenSetCollectionReader {

	public Map<Integer, String> columnNames = null;
	
	/**
	 * Consumes a CSV file and creates a collection of the sets of tokens to be used in similarity search.
	 * @param file  Path to the input CSV file or its URL at a remote server containing the attribute data.
	 * @param colSetId  Ordinal number of the attribute holding the entity identifiers.
	 * @param colSetTokens  Ordinal number of the attribute containing the sets of tokens (e.g., keywords).
	 * @param colDelimiter  Delimiter character between columns in the file.
	 * @param tokDelimiter  Delimiter character between tokens.
	 * @param maxLines  Number of the first lines to read from the file, skipping the rest; if a negative value is specified, all lines will be consumed.
	 * @param header  Boolean indicating that the first line contains the names of the attributes.
	 * @param log  Handle to the log file for keeping messages and statistics.
	 * @return  A collection of the sets of tokens to be used in similarity search.
	 */
	public TokenSetCollection importFromCSVFile(String file, int colSetId, int colSetTokens, String colDelimiter, String tokDelimiter, int maxLines, boolean header, Logger log) {
		TokenSetCollection collection = new TokenSetCollection();
//		List<TokenSet> sets = new ArrayList<TokenSet>();
		int lineCount = 0, errorLines = 0;
		
		// FIXME: Special handling when delimiter appears in an attribute value enclosed in quotes
        String otherThanQuote = " [^\"] ";
        String quotedString = String.format(" \" %s* \" ", otherThanQuote);
        String regex = String.format("(?x) "+ colDelimiter + "(?=(?:%s*%s)*%s*$)", otherThanQuote, quotedString, otherThanQuote);
		
		try {
			// Custom reader to handle either local or remote CSV files
			DataFileReader br = new DataFileReader(file);
			String line;
			String[] columns;
			TokenSet set;

			// If the file has a header, retain the names of the columns for possible future use
			if (header) {
				line = br.readLine();
				columns = line.split(regex,-1);  //colDelimiter+"(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
				columnNames = new HashMap<Integer, String>();
				for (int i = 0; i < columns.length; i++) {
					columnNames.put(i, columns[i]);
				}
			}

			while ((line = br.readLine()) != null) {
				if (maxLines > 0 && lineCount >= maxLines) {
					break;
				}
				try {
					columns = line.split(regex,-1);  //colDelimiter+"(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
					set = new TokenSet();
					// Identifier of the set
					if (colSetId >= 0) {
						set.id = columns[colSetId];
					}
					else
						set.id = String.valueOf(lineCount); 
					//Tokens
					set.tokens = new ArrayList<String>();
					List<String> tokens = new ArrayList<String>(new HashSet<String>(Arrays.asList(columns[colSetTokens].split(tokDelimiter))));
					// FIXME: Custom handling for GDelt tokens with aggregate values
					for (String t : tokens) {
						if (t.indexOf('|') > 0 )
							set.tokens.add(t.substring(0, t.indexOf('|')));
						else
							set.tokens.add(t);
					}
//					set.tokens.addAll(tokens);
					collection.sets.put(set.id, set);
//					sets.add(set);
					lineCount++;
				} catch (Exception e) {
					errorLines++;
				}
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

//		collection.sets = sets.toArray(new TokenSet[0]);

		double elementsPerSet = 0;
		for (TokenSet set : collection.sets.values()) {
			elementsPerSet += set.tokens.size();
		}
		elementsPerSet /= collection.sets.size();

		log.writeln("Finished reading data from file:" + file + ". Lines read: " + lineCount + ". Lines skipped due to errors: "
				+ errorLines + ". Num of sets: " + collection.sets.size() + ". Elements per set: " + elementsPerSet);

		return collection;
	}

	
	/**
	 * Accesses a DBMS table using a JDBC connection and creates a collection of the sets of tokens to be used in similarity search.
	 * @param tableName  Name of the table that holds the attribute data.
	 * @param keyColumnName  Column name of the attribute holding the entity identifiers.
	 * @param valColumnName  Column name of the attribute containing the sets of tokens (e.g., keywords).
	 * @param tokDelimiter  Delimiter character between tokens.
	 * @param jdbcConnector  The JDBC connection that provides access to the table.
	 * @param log  Handle to the log file for keeping messages and statistics.
	 * @return  A collection of the sets of tokens to be used in similarity search.
	 */
	public TokenSetCollection ingestFromJDBCTable(String tableName, String keyColumnName, String valColumnName, String tokDelimiter, JdbcConnector jdbcConnector, Logger log) {

		TokenSetCollection collection = new TokenSetCollection();
 
		// In case no column for key datasetIdentifiers has been specified, use the primary key of the table  
		if (keyColumnName == null) {
			// Assuming that primary key is a single attribute (column), this query can retrieve it 
			// FIXME: Currently working with PostgreSQL only
			keyColumnName = jdbcConnector.getPrimaryKeyColumn(tableName);
			if (keyColumnName == null)  // TODO: Handle other JDBC sources
				return null;
		}
		
		ResultSet rs;	
	  	int n = 0;
		try { 	
			TokenSet set;
			//Execute SQL query in the DBMS and fetch all NOT NULL values available for this attribute
			String sql = "SELECT " + keyColumnName + ", " + valColumnName + " FROM " + tableName + " WHERE " + valColumnName + " IS NOT NULL";
//			System.out.println("CATEGORICAL query: " + sql);
			rs = jdbcConnector.executeQuery(sql); 
			// Iterate through all retrieved results and put them to the in-memory look-up
		    while (rs.next()) {  
		    	set = new TokenSet();
		    	// Identifier of the set
		    	set.id = rs.getString(1);
		    	//Tokens
		    	set.tokens = new ArrayList<String>();
		    	List<String> tokens = new ArrayList<String>(new HashSet<String>(Arrays.asList(rs.getString(2).replaceAll("\n", "").split(tokDelimiter))));
//		    	System.out.println(Arrays.toString(tokens.toArray()));
		    	set.tokens.addAll(tokens);
		    	collection.sets.put(set.id, set);
		    	n++;
		    }
		}
		catch(Exception e) { 
			log.writeln("An error occurred while retrieving data from the database.");
			e.printStackTrace();
		}
			
		double elementsPerSet = 0;
		for (TokenSet set : collection.sets.values()) {
			elementsPerSet += set.tokens.size();
		}
		elementsPerSet /= collection.sets.size();

		log.writeln("Extracted " + n + " data values from database table " + tableName + " regarding column " + valColumnName + ". Num of sets: " + collection.sets.size() + ". Elements per set: " + elementsPerSet);
		
		return collection;
	}

	
	/**
	 * Consumes a CSV file and creates a collection of the sets of tokens to be used as query in similarity search.
	 * @param file  Path to the input CSV file or its URL at a remote server containing the sets of tokens specified in the query.
	 * @param colSetId  Ordinal number of the attribute holding the entity identifiers.
	 * @param colSetTokens  Ordinal number of the attribute containing the sets of tokens (e.g., keywords).
	 * @param colDelimiter  Delimiter character between columns in the file.
	 * @param tokDelimiter  Delimiter character between tokens.
	 * @param maxLines  Number of the first lines to read from the file, skipping the rest; if a negative value is specified, all lines will be consumed.
	 * @param header  Boolean indicating that the first line contains the names of the attributes.
	 * @param tokenizer  The tokenizer used.
	 * @param qgram  The qgram used.
	 * @param queryId  Line number in the file that contains the query specification and is being used as the query identifier. 
	 * @param log  Handle to the log file for keeping messages and statistics.
	 * @return  A collection of the sets of tokens to be used as the query specification in similarity search.
	 */
	public TokenSetCollection createQueryFromCSVFile(String file, int colSetId, int colSetTokens, String colDelimiter, String tokDelimiter, int maxLines, boolean header, String tokenizer, int qgram, int queryId, Logger log) {
		TokenSetCollection collection = new TokenSetCollection();
//		List<TokenSet> sets = new ArrayList<TokenSet>();
		int lineCount = 0, errorLines = 0;
		try {
			// Custom reader to handle either local or remote CSV files
			DataFileReader br = new DataFileReader(file);
			String line;
			String[] columns;
			TokenSet set;

			// If the file has header, ignore the first line
			if (header) {
				br.readLine();
			}

			while ((line = br.readLine()) != null) {
				
				lineCount++;
				if (lineCount < queryId)
					continue;
				if ((maxLines > 0 && lineCount > maxLines) || (lineCount > queryId)) {
					break;
				}
				try {
					columns = line.split(colDelimiter);
					set = new TokenSet();
					set.id = columns[colSetId];
					// DatasetIdentifier of the set
					if (colSetId >= 0) {
						set.id = columns[colSetId];
					}
					else
						set.id = String.valueOf(lineCount); 
					set.tokens = new ArrayList<String>();
/*
					//FIXME: Commented out this block because the use of qgram was not clear
					if (tokenizer.equals("qgram")) {
						set.originalString = columns[colSetTokens];
						Reader reader = new StringReader(columns[colSetTokens]);
						NGramTokenizer gramTokenizer = new NGramTokenizer(reader, qgram, qgram);
						CharTermAttribute charTermAttribute = gramTokenizer.addAttribute(CharTermAttribute.class);
						while (gramTokenizer.incrementToken()) {
							set.tokens.add(charTermAttribute.toString());
						}
						gramTokenizer.end();
						gramTokenizer.close();
					} else */ {
//						List<String> tokens = Arrays.asList(columns[colSetTokens].split(tokDelimiter));
						List<String> tokens = new ArrayList<String>(new HashSet<String>(Arrays.asList(columns[colSetTokens].split(tokDelimiter))));
//					set.tokens.addAll(new HashSet<String>(tokens));
						set.tokens.addAll(tokens);
					}
//					sets.add(set);
					collection.sets.put(set.id, set);

				} catch (Exception e) {
					errorLines++;
				}
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

//		collection.sets = sets.toArray(new TokenSet[0]);

		double elementsPerSet = 0;
		for (TokenSet set : collection.sets.values()) {
			elementsPerSet += set.tokens.size();
		}
		elementsPerSet /= collection.sets.size();

		log.writeln("Finished reading query from file: " + file + ". Lines read: " + lineCount + ". Lines skipped due to errors: "
				+ errorLines + ". Num of sets: " + collection.sets.size() + ". Elements per set: " + elementsPerSet);

		return collection;
	}
	

	/**
	 * Accepts a string of keywords and creates a collection of the sets of tokens to be used as query in similarity search.
	 * @param keywords  A string containing the keywords to be used as tokens.
	 * @param tokDelimiter  Delimiter character between keywords.
	 * @param log  Handle to the log file for keeping messages and statistics.
	 * @return  A collection of the sets of tokens to be used as the query specification in similarity search.
	 */
	public TokenSetCollection createFromQueryKeywords(String keywords, String tokDelimiter, Logger log) {
		TokenSetCollection collection = new TokenSetCollection();
//		List<TokenSet> sets = new ArrayList<TokenSet>();

		int errorLines = 0;
		try {
			TokenSet set;

			if (keywords != null) {

				try {
//					System.out.println(keywords);
					set = new TokenSet();
					set.id = "querySet"; 
					set.tokens = new ArrayList<String>();
					List<String> tokens = new ArrayList<String>(new HashSet<String>(Arrays.asList(keywords.split(tokDelimiter))));
					set.tokens.addAll(tokens);
//					sets.add(set);
					collection.sets.put(set.id, set);

				} catch (Exception e) {
					errorLines++;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

//		collection.sets = sets.toArray(new TokenSet[0]);

		double elementsPerSet = 0;
		for (TokenSet set : collection.sets.values()) {
			elementsPerSet += set.tokens.size();
		}
		elementsPerSet /= collection.sets.size();

		log.writeln("Finished reading query keywords: " + keywords + ". Errors: "
				+ errorLines + ". Num of sets: " + collection.sets.size() + ". Elements per set: " + elementsPerSet);

		return collection;
	}
	

	/**
	 * Accepts an array of keywords and creates a collection of the sets of tokens to be used as query in similarity search.
	 * @param keywords  An array of keywords to be used as tokens.
	 * @param log  Handle to the log file for keeping messages and statistics.
	 * @return  A collection of the sets of tokens to be used as the query specification in similarity search.
	 */
	public TokenSetCollection createFromQueryKeywords(String[] keywords, Logger log) {
		
		TokenSetCollection collection = new TokenSetCollection();

		int errorLines = 0;
		try {
			TokenSet set;

			if (keywords != null) {

				try {
//					System.out.println(keywords);
					set = new TokenSet();
					set.id = "querySet"; 
					set.tokens = new ArrayList<String>();
					List<String> tokens = new ArrayList<String>(Arrays.asList(keywords));
					set.tokens.addAll(tokens);
//					sets.add(set);
					collection.sets.put(set.id, set);

				} catch (Exception e) {
					errorLines++;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

//		collection.sets = sets.toArray(new TokenSet[0]);

		double elementsPerSet = 0;
		for (TokenSet set : collection.sets.values()) {
			elementsPerSet += set.tokens.size();
		}
		elementsPerSet /= collection.sets.size();

		log.writeln("Finished reading query keywords: " + Arrays.toString(keywords) + ". Errors: "
				+ errorLines + ". Num of sets: " + collection.sets.size() + ". Elements per set: " + elementsPerSet);

		return collection;
	}
	
	
	public int determineQ(String file1, String file2, int colSetTokens, String colDelimiter, int maxLines,
			boolean header, double threshold) {

		int min1 = findMinString(file1, colSetTokens, colDelimiter, maxLines, header);
		int min2 = (file2 == null) ? min1 : findMinString(file2, colSetTokens, colDelimiter, maxLines, header);
		int max = (min1 >= min2) ? min1 : min2;
		int qgram = (int) Math.floor(max * threshold);
		return (qgram==0) ? 1: qgram;
	}


	public int findMinString(String file, int colSetTokens, String colDelimiter, int maxLines, boolean header) {

		int lineCount = 0, errorLines = 0, min = 0;
		try {
			// Custom reader to handle either local or remote CSV files
			DataFileReader br = new DataFileReader(file);
			String line;
			String[] columns;

			// if the file has header, ignore the first line
			if (header) {
				br.readLine();
			}

			while ((line = br.readLine()) != null) {
				if (maxLines > 0 && lineCount >= maxLines) {
					break;
				}
				try {
					columns = line.split(colDelimiter);
					if (lineCount == 0)
						min = columns[colSetTokens].length();
					else {
						if (columns[colSetTokens].length() < min)
							min = columns[colSetTokens].length();
					}

					lineCount++;
				} catch (Exception e) {
					errorLines++;
				}
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return min;
	}
	
	
	/**
	 * Creates an inverted index against the given collection of sets of tokens.
	 * FIXME: queryCollection is NOT known when the index is built
	 * @param targetCollection  A collection of sets of tokens to be indexed.
	 * @param log  Handle to the log file for keeping messages and statistics.
	 * @return  An inverted index to facilitate similarity search operations against the sets of tokens.
	 */
	public InvertedIndex buildInvertedIndex(TokenSetCollection targetCollection, Logger log) {
		
		InvertedIndex index = new InvertedIndex();
		
		TIntList[] idx;
		
		//Used in categorical similarity search only	
		IntSetCollection transformedTargetCollection;
//		IntSetCollection transformedQueryCollection;
		
		// Transform the input collections
		long duration = System.nanoTime();
		CollectionTransformer transformer = new CollectionTransformer();
		// Create a global collection of tokens for the dictionary
//		TokenSetCollection totalCollection = new TokenSetCollection();		
//		totalCollection.sets.putAll(targetCollection.sets);
//		totalCollection.sets.putAll(queryCollection.sets);
		// Dictionary must be common for the target and query set collections
		// FIXME: This will not be possible if the query tokens are NOT known when the index is built
		TObjectIntMap<String> tokenDictionary = transformer.constructTokenDictionary(targetCollection);
		transformedTargetCollection = transformer.transformCollection(targetCollection, tokenDictionary);
//		transformedQueryCollection = transformer.transformCollection(queryCollection, tokenDictionary);
		duration = System.nanoTime() - duration;
		log.writeln("Transformation time: " + duration / 1000000000.0 + " sec.");
		
		// Inverted index initialization
		duration = System.nanoTime();
		idx = new TIntList[transformedTargetCollection.numTokens];
		for (int i = 0; i < idx.length; i++) {
			idx[i] = new TIntArrayList();
		}

		// Inverted index construction
		for (int i = 0; i < transformedTargetCollection.sets.length; i++) {
			// Since no threshold is known beforehand, index is constructed with
			// all tokens (not prefixes)
			for (int j = 0; j < transformedTargetCollection.sets[i].length; j++) {
				idx[transformedTargetCollection.sets[i][j]].add(i);
			}
		}
		duration = System.nanoTime() - duration;
		log.writeln("Inverted index build time: " + duration / 1000000000.0 + " sec.");
		
		// Keep this in an index structure for use in similarity search queries
		index.idx = idx;
		index.transformedTargetCollection = transformedTargetCollection;
		index.tokenDictionary = tokenDictionary;
		
		return index;
	}	

}
