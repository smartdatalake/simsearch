package eu.smartdatalake.simsearch.manager.ingested.categorical;

public class FilterUtils {

	public int getSizeLowerBound(int size, String similarity, double threshold) {
		/*
		 * Computes lower bound for size filter.
		 * 
		 * Specifically, computes lower bound on the number of tokens a string must have
		 * in order to obtain a similarity score that satisfies the input threshold with
		 * a string containing number of tokens, specified by "size".
		 * 
		 * References: String Similarity Joins: An Experimental Evaluation, VLDB 2014.
		 */
		if (similarity.equalsIgnoreCase("cosine"))
			return (int) Math.ceil(threshold * threshold * size);
		else if (similarity.equalsIgnoreCase("dice"))
			return (int) Math.ceil(threshold / (2 - threshold) * size);
		else if (similarity.equalsIgnoreCase("neds"))
			return (int) Math.ceil(threshold * size);
		else if (similarity.equalsIgnoreCase("jaccard"))
			return (int) Math.ceil(threshold * size);

//		else if (similarity.equalsIgnoreCase("overlap")
//			return (int) threshold;
		return -1;
	}

	public int getSizeUpperBound(int size, String similarity, double threshold) {
		/*
		 * Computes upper bound for size filter.
		 * 
		 * Specifically, computes upper bound on the number of tokens a string must have
		 * in order to obtain a similarity score that satisfies the input threshold with
		 * a string containing number of tokens, specified by "size".
		 * 
		 * References: String Similarity Joins: An Experimental Evaluation, VLDB 2014.
		 */

		if (similarity.equalsIgnoreCase("cosine"))
			return (int) Math.floor(size / (threshold * threshold));
		else if (similarity.equalsIgnoreCase("dice"))
			return (int) Math.floor(((2 - threshold) / threshold) * size);
		else if (similarity.equalsIgnoreCase("neds"))
			return (int) Math.floor(size / threshold);
		else if (similarity.equalsIgnoreCase("jaccard"))
			return (int) Math.floor(size / threshold);
//	    else if (similarity.equalsIgnoreCase("overlap")
//	        return (int) maxsize;
		return -1;
	}

	public int getPrefixLength(int leftSize, int rightSize, String similarity, double threshold, int qgram) {
		/*
		 * Computes prefix length.
		 * 
		 * References: String Similarity Joins: An Experimental Evaluation, VLDB 2014.
		 */

		if (leftSize == 0)
			return 0;

		if (similarity.equalsIgnoreCase("neds")) {
			int p = leftSize - rightSize + 1;
			p = (p <= leftSize) ? p : leftSize;
			return p - 1;
//			int max = (leftSize >= rightSize) ? leftSize : rightSize;
//			return (int)Math.ceil(qgram*(1-threshold)*max);
		}

		return leftSize - rightSize + 1;
	}

	public int getOverlapThreshold(int leftSize, int rightSize, String similarity, double threshold, int qgram) {
		/*
		 * Computes the minimum overlap needed between the tokens to satisfy the
		 * threshold.
		 */

		if (similarity.equalsIgnoreCase("cosine"))
			return (int) Math.ceil(threshold * Math.sqrt(leftSize * rightSize));
		else if (similarity.equalsIgnoreCase("dice"))
			return (int) Math.ceil((threshold / 2) * (leftSize + rightSize));
		else if (similarity.equalsIgnoreCase("neds")) {
			int max = (leftSize >= rightSize) ? leftSize : rightSize;
			int ov = (int) Math.ceil(Math.round((max + 1 - qgram - qgram * (1 - threshold) * max) * 100000) / 100000.0);
			return max-qgram+1-ov;
		} else if (similarity.equalsIgnoreCase("jaccard"))
			return (int) Math
					.ceil(Math.round((threshold / (1 + threshold)) * (leftSize + rightSize) * 100000) / 100000.0);
		return -1;
	}

	public int getLength(int set_len, String string, String similarity) {
		return similarity.equalsIgnoreCase("neds") ? string.length() : set_len;
	}
}
