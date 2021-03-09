package eu.smartdatalake.simsearch.pivoting.rtree;


public final class Candidate<R, D> {

	R node;
	D distance;
	
	Candidate(R n, D d) {
		node = n;
		distance = d;
	}
	
	public R node() {
		return node;
	}
	
	public D distance() {
		return distance;
	}
}
