package eu.unitn.disi.db.grava.graphs;

import java.util.HashSet;

public class QueryAnwser {
	
	private HashSet<Edge> edges;
	
	public QueryAnwser() {
		edges = new HashSet<Edge>();
	}
	
	public QueryAnwser(HashSet<Edge> edges){
		this.edges = edges;
	}
	
	public void addEgde(Edge e){
		edges.add(e);
	}

}
