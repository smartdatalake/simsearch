package eu.smartdatalake.simsearch.categorical;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class TokenSetCollectionReader {

	public TokenSetCollection importFromFile(String file, int colSetId, int colSetTokens, String colDelimiter,
			String tokDelimiter, int maxLines, boolean header, PrintStream logStream) {
		TokenSetCollection collection = new TokenSetCollection();
//		List<TokenSet> sets = new ArrayList<TokenSet>();
		int lineCount = 0, errorLines = 0;
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line;
			String[] columns;
			TokenSet set;

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
					set = new TokenSet();
					//Identifier of the set
					if (colSetId >= 0) {
						set.id = columns[colSetId];
					}
					else
						set.id = String.valueOf(lineCount); 
					//Tokens
					set.tokens = new ArrayList<String>();
					List<String> tokens = new ArrayList<String>(new HashSet<String>(Arrays.asList(columns[colSetTokens].split(tokDelimiter))));
					set.tokens.addAll(tokens);
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

		logStream.println("Finished reading data from file. Lines read: " + lineCount + ". Lines skipped due to errors: "
				+ errorLines + ". Num of sets: " + collection.sets.size() + ". Elements per set: " + elementsPerSet);

		return collection;
	}

	public TokenSetCollection createFromQueryFile(String file, int colSetId, int colSetTokens, String colDelimiter,
			String tokDelimiter, int maxLines, boolean header, String tokenizer, int qgram, int queryId, PrintStream logStream) {
		TokenSetCollection collection = new TokenSetCollection();
//		List<TokenSet> sets = new ArrayList<TokenSet>();
		int lineCount = 0, errorLines = 0;
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line;
			String[] columns;
			TokenSet set;

			// if the file has header, ignore the first line
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
					logStream.println(line);
					columns = line.split(colDelimiter);
					set = new TokenSet();
					set.id = columns[colSetId];
					//Identifier of the set
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

		logStream.println("Finished reading query from file. Lines read: " + lineCount + ". Lines skipped due to errors: "
				+ errorLines + ". Num of sets: " + collection.sets.size() + ". Elements per set: " + elementsPerSet);

		return collection;
	}
	

	public TokenSetCollection createFromQueryKeywords(String keywords, String tokDelimiter, PrintStream logStream) {
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

		logStream.println("Finished reading query keywords: " + keywords + ". Errors: "
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
			BufferedReader br = new BufferedReader(new FileReader(file));
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
}