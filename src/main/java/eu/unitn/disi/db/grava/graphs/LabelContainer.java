package eu.unitn.disi.db.grava.graphs;

import java.util.HashSet;

import eu.unitn.disi.db.grava.utils.StdOut;

public class LabelContainer {
	private long labelID;
	private HashSet<Long> nodes;
	private int frequency;
	
	public LabelContainer(){
		this.labelID = -1;
		this.nodes = new HashSet<Long>();
		this.frequency = 0;
	}
	
	public LabelContainer(long labelID){
		this.labelID = labelID;
		this.nodes = new HashSet<Long>();
		this.frequency = 0;
	}
	
	public LabelContainer(long labelID, HashSet<Long> nodes){
		this.labelID = labelID;
		this.nodes = nodes;
		this.frequency = 0;
	}
	
	public void addNode(Long node){
		frequency ++;
		nodes.add(node);
	}
	
	public boolean removeNode(Long node){
		if(nodes.contains(node)){
			nodes.remove(node);
			return true;
		}else{
			return false;
		}
	}

	public long getLabelID() {
		return labelID;
	}

	public void setLabelID(long labelID) {
		this.labelID = labelID;
	}

	public HashSet<Long> getNodes() {
		return nodes;
	}

	public void setNodes(HashSet<Long> nodes) {
		this.nodes = nodes;
	}

	public int getFrequency() {
		return frequency;
	}

	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}
	
	public void print(){
		StdOut.println("Label:" + this.labelID + ", frequency:" + this.frequency);
		for(Long temp : this.nodes){
			StdOut.println(temp);
		}
	}
	
}
