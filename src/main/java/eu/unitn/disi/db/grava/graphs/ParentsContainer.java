package eu.unitn.disi.db.grava.graphs;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

public class ParentsContainer {
	HashSet<LabelParents> container;
	
	public ParentsContainer() {
		container = new HashSet<LabelParents>();
	}
	
	public boolean contains(Long label, HashMap<Long, Integer> parents){
		for(LabelParents lp : container){
			if(lp.getLabel().equals(label) && lp.getParents().equals(parents)){
				return true;
			}
		}
		return false;
	}
	
	public boolean add(LabelParents lp){
		return container.add(lp);
	}
	
	public LabelParents get(Long label, HashMap<Long, Integer> parents){
		for(LabelParents lp : container){
			if(lp.getLabel().equals(label) && lp.getParents().equals(parents)){
				return lp;
			}
		}
		return null;
	}
	
	public void print(){
		for(LabelParents lp : container){
			HashMap<Long, Integer> parents = lp.getParents();
			System.out.println("label:" + lp.getLabel());
			for(Entry<Long, Integer> p : parents.entrySet()){
				System.out.println("parent:" + p.getKey() + " freq:" + p.getValue());
			}
			System.out.println("label paretns freq:" + lp.getFreq());
		}
	}
	
	public boolean remove(Long label, HashMap<Long, Integer> parents){
		for(LabelParents lp : container){
			if(lp.getLabel().equals(label) && lp.getParents().equals(parents)){
				return container.remove(lp);
			}
		}
		return false;
	}

	public HashSet<LabelParents> getContainer() {
		return container;
	}

	public void setContainer(HashSet<LabelParents> container) {
		this.container = container;
	}
	

}
