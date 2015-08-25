package eu.unitn.disi.db.grava.graphs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Answer {
	private ArrayList<String> answerEdge;
	
	public Answer(){
		answerEdge = new ArrayList<>();
	}
	
	public Answer(ArrayList<String> answerEdge){
		this.answerEdge = answerEdge;
	}
	
	public boolean add(String edge){
		return answerEdge.add(edge);
	}
	
	public int hashCode(){
		return answerEdge.size();
	}
	
	public boolean equals(Object o){
		Answer ans = (Answer)o;
		ArrayList<String> edges = ans.getAnswerEdge();
		if(edges.size() != this.answerEdge.size()){
			return false;
		}
		for(int i = 0; i < edges.size(); i++){
			if(!answerEdge.contains(edges.get(i))){
				return false;
			}
		}
		return true;
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		for(String edge : this.answerEdge){
			sb.append(edge.toString());
			sb.append(System.getProperty("line.separator"));
		}
		return sb.toString();
	}
	public ArrayList<String> getAnswerEdge() {
		return answerEdge;
	}

	public void setAnswerEdge(ArrayList<String> answerEdge) {
		this.answerEdge = answerEdge;
	}
	
}
