package eu.smartdatalake.simsearch.csv.lookup;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

import eu.smartdatalake.simsearch.Assistant;
import eu.smartdatalake.simsearch.Constants;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Point;

/**
 * Implements a Word2Vector transformer for sets of keywords (tokens) into a vector of numerical values.
 * It makes use of a user-specified dictionary with vector representations for a vocabulary of individual words.
 */
public class Word2VectorTransformer {

	Assistant myAssistant;
	private int sizeVector;
	private Map<String, double[]> vectorDictionary;
	public long numMissingKeywords;
	
	/**
	 * Constructor
	 * @param vectorDictionary  The dictionary of vectors corresponding to words in a vocabulary.
	 * @param sizeVector  Cardinality of the vector to be created.
	 */
	public Word2VectorTransformer(Map<String, double[]> vectorDictionary, int sizeVector) {
		
		myAssistant = new Assistant();
		this.vectorDictionary = vectorDictionary;
		this.sizeVector = sizeVector;
		this.numMissingKeywords = 0;
	}
	
	/**
	 * Provides a vector for the given array of tokens (keywords) based on their representations in the dictionary.
	 * @param tokens   An array of string values (e.g., a set of keywords).
	 * @return  A vector representation for the given array. 
	 */
	public double[] getVector(String[] tokens) {
		
		double[] vec = new double[sizeVector];
		
		// If tokens exist, prepare a vector of double values
		if (tokens.length > 0) {
			
			Arrays.fill(vec, 0.0);
			// Define element-wise summation over two arrays
			DoubleBinaryOperator opSum = (x,y) -> x+y;
			
			// CAUTION! Assuming that the dictionary consists of lower-case keywords
			for (String token: tokens) {
				double[] tVector;
				if (token.contains(Constants.WORD_DELIMITER))
					tVector = getVector(token.split("\\" + Constants.WORD_DELIMITER));  // Recursive call over the word components
				else
					tVector = vectorDictionary.get(token.toLowerCase());
				
				if (tVector != null)
					vec = myAssistant.applyOnDoubleArrays(opSum, vec, tVector);
				else
					this.numMissingKeywords++;
			}
				
			// Define element-wise average over the vector
			DoubleUnaryOperator opAvg = (x -> x/tokens.length);
			vec = myAssistant.applyOnDoubleArray(opAvg, vec);
			
//			System.out.println(Arrays.toString(tokens) + " -> " + Arrays.toString(vec));
		}
		else   // A vector of NULL values
			Arrays.fill(vec, Double.NaN);
		
		return vec;
	}
	
	
	/**
	 * Apply transformation on the given dataset of sets of keywords.
	 * @param inData  The input dataset of sets of keywords (each with a unique identifier as key).
	 * @return  The output dataset of transformed vectors representated as multi-dimensional points (using the same identifiers as keys and double ordinates in each value).
	 */
	public Map<String, Point> apply(Map<String, String[]> inData) {
		
		TreeMap<String, Point> outData = new TreeMap<String, Point>();
		
		// Transform each set of keywords into an array of double values
		for (Map.Entry<String, String[]> entry : inData.entrySet()) {
			outData.put(entry.getKey(), Point.create(getVector(entry.getValue())));
		}

		return outData;
	}
		
}
