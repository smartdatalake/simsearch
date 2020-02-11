package eu.smartdatalake.simsearch.categorical;


import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import info.debatty.java.stringsimilarity.NormalizedLevenshtein;

public class Verification {

	public boolean verifyJaccardOpt(int[] r, int[] s, int minOverlap) {

		int olap = 0, pr = 0, ps = 0;

		int maxr = r.length - pr + olap;
		int maxs = s.length - ps + olap;

		while (maxr >= minOverlap && maxs >= minOverlap && olap < minOverlap) {
			if (r[pr] == s[ps]) {
				pr++;
				ps++;
				olap++;
			} else if (r[pr] < s[ps]) {
				pr++;
				maxr--;
			} else {
				ps++;
				maxs--;
			}
		}
		return olap >= minOverlap;
	}

	public double verifyWithScore(String r, String s, String similarity) {
		NormalizedLevenshtein l = new NormalizedLevenshtein(); 
		return 1 - l.distance(r, s);
	}

	public double verifyWithScore(int[] r, int[] s) {

		int olap, pr, ps, maxr, maxs;

		olap = 0;
		pr = 0;
		ps = 0;
		maxr = r.length - pr + olap;
		maxs = s.length - ps + olap;

		while (maxr > olap && maxs > olap) {

			if (r[pr] == s[ps]) {
				pr++;
				ps++;
				olap++;
			} else if (r[pr] < s[ps]) {
				pr++;
				maxr--;
			} else {
				ps++;
				maxs--;
			}
		}

		return (double) (olap / (1.0 * (r.length + s.length - olap)));
	}

	public double verifyJaccardBasic(int[] r, int[] s) {
		
		TIntSet intersection = new TIntHashSet(r);
		intersection.retainAll(s);
		TIntSet union = new TIntHashSet(r);
		union.addAll(s);

		return ((double) intersection.size()) / ((double) union.size());
	}
	
	public double verifyWithScore(int[] r, int[] s, String similarity) {
		if (similarity.equalsIgnoreCase("jaccard"))
			return verifyWithScore(r, s);
		else if (similarity.equalsIgnoreCase("dice")) {
			TIntSet intersection = new TIntHashSet(r);
			intersection.retainAll(s);
			return 2*intersection.size() / (r.length+s.length);
		}else if (similarity.equalsIgnoreCase("cosine")) {
			TIntSet intersection = new TIntHashSet(r);
			intersection.retainAll(s);
			return intersection.size() / Math.sqrt(r.length*s.length);
		}
		return verifyWithScore(r, s);	
	}
}