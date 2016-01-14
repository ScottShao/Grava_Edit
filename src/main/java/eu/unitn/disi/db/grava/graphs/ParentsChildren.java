package eu.unitn.disi.db.grava.graphs;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;



public class ParentsChildren {
	private HashMap<HashMap, HashMap> parentsChildren;
	private HashMap<Long, LabelContainer> labelFreq;
	private int totalEdgeNum;
	
	public ParentsChildren() {
		parentsChildren = new HashMap<HashMap, HashMap>();
	}
	
	public void add(HashMap<Long, Integer> parents, HashMap<Long, Integer> children){
		if(parentsChildren.containsKey(parents)){
			HashMap<Long, Integer> old = parentsChildren.get(parents);
			for(Entry<Long, Integer> child : children.entrySet()){
				if(old.containsKey(child.getKey())){
					int freq = old.get(child.getKey());
					freq += child.getValue();
					old.put(child.getKey(), freq);
				}else{
					old.put(child.getKey(), child.getValue());
				}
			}
			parentsChildren.put(parents, old);
		}else{
			parentsChildren.put(parents, children);
		}
	}
	
	public double computeSel(Long label){
		int lFreq = labelFreq.get(label).getFrequency();
		return ((double)lFreq)/totalEdgeNum;
	}
	
	public double computeSel(HashMap<Long, Integer> pLabels, Long cLabel){
		int childFreq = 0;
		int totalFreq = 0;
		HashMap<Long, Integer> parents;
		HashMap<Long, Integer> children;
		boolean isContained;
		Long label;
		int freq;
		for(Entry<HashMap, HashMap> en : parentsChildren.entrySet()){
			parents = en.getKey();
			children = en.getValue();
			isContained = true;
			for(Long l : pLabels.keySet()){
				if(!parents.containsKey(l) && parents.get(l) < pLabels.get(l)){
					isContained = false;
					break;
				}
			}
			if(isContained){
				for(Entry<Long, Integer> cEn : children.entrySet()){
					label = cEn.getKey();
					freq = cEn.getValue();
					totalFreq += freq;
					if(label.equals(cLabel)){
						childFreq += freq;
					}
				}
			}
		}
		return ((double)childFreq)/totalFreq;
	}
	
	public void print(){
		for(Entry<HashMap, HashMap> en : parentsChildren.entrySet()){
			HashMap<Long, Integer> parents = en.getKey();
			HashMap<Long, Integer> children = en.getValue();
			System.out.println("Parents:");
			for(Entry<Long, Integer> pEn : parents.entrySet()){
				System.out.println(pEn.getKey() + " " + pEn.getValue());
			}
			System.out.println("Children:");
			for(Entry<Long, Integer> cEn : children.entrySet()){
				System.out.println(cEn.getKey() + " " + cEn.getValue());
			}
		}
	}

}
