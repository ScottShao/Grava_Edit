package eu.unitn.disi.db.grava.graphs;

import java.util.HashMap;
import java.util.Map;

public class Answer {
	Map<Edge, Edge> mappingEdges;
	
	public Answer() {
		mappingEdges = new HashMap<>();
	}
	
	public void put(Edge qEdge, Edge gEdge){
		this.mappingEdges.put(qEdge, gEdge);
	}

	public Map<Edge, Edge> getMappingEdges() {
		return mappingEdges;
	}

	public void setMappingEdges(Map<Edge, Edge> mappingEdges) {
		this.mappingEdges = mappingEdges;
	}
	
	public Edge get(Edge edge){
		return this.mappingEdges.get(edge);
	}
	public void clear(){
		this.mappingEdges.clear();
	}
}
