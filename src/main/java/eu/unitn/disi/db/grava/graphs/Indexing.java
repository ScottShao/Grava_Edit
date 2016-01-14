package eu.unitn.disi.db.grava.graphs;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

public class Indexing {
	private HashMap<ParentChild, Integer> pcs;
	private HashMap<Long, Integer> parentFreq;
	int totalParentFreq;
	private BigMultigraph query;
	
	public Indexing(){
		pcs = new HashMap<ParentChild, Integer>();
		totalParentFreq = 0;
		parentFreq = new HashMap<Long, Integer>();
	}
	
	public void computeSel(HashSet<Long> nodes){
		Collection<Edge> in;
		Collection<Edge> out;
		HashMap<Long, Integer> parents;
		HashMap<Long, Integer> children;
		for(Long node : nodes){
			in = query.incomingEdgesOf(node);
			out = query.outgoingEdgesOf(node);
			if(in.size() != 0 && out.size() != 0){
				parents = new HashMap<Long, Integer>();
				children = new HashMap<Long, Integer>();
			}
		}
	}
	
	public void indexing(BigMultigraph graph){
		Collection<Long> vertexes = graph.vertexSet();
		Collection<Edge> ins;
		Collection<Edge> outs;
		ParentChild pc;
		int freq;
		Long pLabel;
		Long cLabel;
		HashMap<Long, Integer> maxRep = graph.getMaxRep();
		HashMap<Long, Integer> temp = new HashMap<Long, Integer>();
		for(Long vertex : vertexes){
			ins = graph.incomingEdgesOf(vertex);
			outs = graph.outgoingEdgesOf(vertex);
			
			for(Edge in : ins){
				if(temp.containsKey(in.getLabel())){
					temp.put(in.getLabel(), temp.get(in.getLabel())+1);
				}else{
					temp.put(in.getLabel(), 1);
				}
			}
			for(Entry<Long,Integer> en : temp.entrySet()){
				if(maxRep.containsKey(en.getKey())){
					if(maxRep.get(en.getKey()) < en.getValue()){
						maxRep.put(en.getKey(), en.getValue());
					}
				}else{
					maxRep.put(en.getKey(), en.getValue());
				}
			}
			temp.clear();
			for(Edge out : outs){
				if(temp.containsKey(out.getLabel())){
					temp.put(out.getLabel(), temp.get(out.getLabel())+1);
				}else{
					temp.put(out.getLabel(), 1);
				}
			}
			for(Entry<Long,Integer> en : temp.entrySet()){
				if(maxRep.containsKey(en.getKey())){
					if(maxRep.get(en.getKey()) < en.getValue()){
						maxRep.put(en.getKey(), en.getValue());
					}
				}else{
					maxRep.put(en.getKey(), en.getValue());
				}
			}
			temp.clear();
			if(ins.size() != 0 && outs.size() != 0){
				for(Edge in : ins){
					pLabel = in.getLabel();
					totalParentFreq += outs.size();
					if(parentFreq.containsKey(pLabel)){
						freq = parentFreq.get(pLabel);
					}else{
						freq = 0;
					}
					freq += outs.size();
					parentFreq.put(pLabel, freq);
					for(Edge out : outs){
						cLabel = out.getLabel();
						pc = new ParentChild(pLabel, cLabel);
						if(pcs.containsKey(pc)){
							freq = pcs.get(pc);
						}else{
							freq = 0;
						}
						freq ++;
						pcs.put(pc, freq);
						
					}
				}
			}else{
				continue;
			}
		}
//		for(Entry<Long,Integer> en : maxRep.entrySet()){
//			System.out.println("label:" + en.getKey() + " max repeatition:" + en.getValue());
//		}
//		this.print();
	}

	public void print(){
		System.out.println("Parent frequency:");
		for(Entry<Long, Integer> en : parentFreq.entrySet()){
			System.out.println(en.getKey() + " " + en.getValue());
		}
		System.out.println("Parent child frequency:");
		for(Entry<ParentChild, Integer> en : pcs.entrySet()){
			System.out.println(en.getKey()+ " " + en.getValue());
		}
	}
	

	public HashMap<ParentChild, Integer> getPcs() {
		return pcs;
	}

	public void setPcs(HashMap<ParentChild, Integer> pcs) {
		this.pcs = pcs;
	}

	public BigMultigraph getQuery() {
		return query;
	}

	public void setQuery(BigMultigraph query) {
		this.query = query;
	}

	public HashMap<Long, Integer> getParentFreq() {
		return parentFreq;
	}

	public void setParentFreq(HashMap<Long, Integer> parentFreq) {
		this.parentFreq = parentFreq;
	}

	public int getTotalParentFreq() {
		return totalParentFreq;
	}

	public void setTotalParentFreq(int totalParentFreq) {
		this.totalParentFreq = totalParentFreq;
	}
	
	
	
}
