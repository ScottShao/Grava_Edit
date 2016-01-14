package eu.unitn.disi.db.grava.graphs;

import java.util.HashMap;
import java.util.HashSet;

public class LabelParents {
	private Long label;
	private HashMap<Long, Integer> parents;
	private int freq;
	public LabelParents() {
		label = -1L;
		parents = null;
		freq = -1;
	}
	
	public LabelParents(Long label){
		this.label = label;
		this.parents = new HashMap<Long, Integer>();
		this.freq = 0;
	}
	
	public LabelParents(Long label, HashMap<Long, Integer> parents, int freq){
		this.label = label;
		this.parents = parents;
		this.freq = freq;
	}
	
	public int hashCode(){
		return (int)(label%10000);
	}
	
	public boolean equals(Object o){
		LabelParents lp = (LabelParents)o;
		if(lp.getFreq() == freq && lp.getParents().equals(parents) && lp.getLabel().equals(label)){
			return true;
		}else{
			return false;
		}
		
	}
	public Long getLabel() {
		return label;
	}
	public void setLabel(Long label) {
		this.label = label;
	}
	
	public HashMap<Long, Integer> getParents() {
		return parents;
	}

	public void setParents(HashMap<Long, Integer> parents) {
		this.parents = parents;
	}

	public int getFreq() {
		return freq;
	}
	public void setFreq(int freq) {
		this.freq = freq;
	}

}
