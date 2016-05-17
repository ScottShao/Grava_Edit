package eu.unitn.disi.db.grava.graphs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import eu.unitn.disi.db.grava.vectorization.NeighborTables;

public class Selectivity {
	private ArrayList<Long> visitSeq;
	private Multigraph query;
	private BigMultigraph graph;
	private double selectivity;
	private Indexing indexing;
	private Long startingNode;
	private HashMap<Long, Double> sels;
	private HashMap<Long, HashSet<Edge>> paths;
	private NeighborTables queryTables;
	private NeighborTables graphTables;
	private double pruningCost;
	private double updateCost;
	
	public Selectivity() {
		indexing = new Indexing();
		sels = new HashMap<Long, Double>();
		pruningCost = 0;
		updateCost = 0;
		selectivity = 1;
	}
	
	
	public void print(){
		for(Entry<Long, Double> en: sels.entrySet()){
			System.out.println("node:" + en.getKey() + " sel:" + en.getValue());
		}
	}
	public void compute(){
		pruningCost += this.computeNeighbourNumber(startingNode)*graph.vertexSet().size()*Math.log(graph.edgeSet().size())/Math.log(2);
		this.computSelectivity(1, startingNode);
		this.computSelPathNotCorrelated(1, startingNode);
		this.computSelAllNotCorrelated(1, startingNode);
//		this.computePruningCost(startingNode);
//		this.computeUpdateCost(startingNode);
	}
	public void computePruningCost(Long cur){
		HashSet<Edge> ps = paths.get(cur);
		for(Edge e : ps){
			if(e.getDestination().equals(e.getSource())){
				continue;
			}
			if(e.getDestination().equals(cur)){
				pruningCost += sels.get(cur)*this.computeNeighbourNumber(e.getSource())*graph.vertexSet().size()*Math.log(graph.edgeSet().size())/Math.log(2);
				this.computePruningCost(e.getSource());
			}else{
				pruningCost += sels.get(cur)*this.computeNeighbourNumber(e.getDestination())*graph.vertexSet().size()*Math.log(graph.edgeSet().size())/Math.log(2);
				this.computePruningCost(e.getDestination());
			}
		}
	}
	
	public void computeUpdateCost(Long cur){
		HashSet<Edge> ps = paths.get(cur);
		updateCost += sels.get(cur)*graph.vertexSet().size()*graph.getMaxDegree();
		for(Edge e : ps){
			if(e.getDestination().equals(e.getSource())){
				continue;
			}
			if(e.getDestination().equals(cur)){
				this.computeUpdateCost(e.getSource());
			}else{
				this.computeUpdateCost(e.getDestination());
			}
		}
	}
	
	public void computSelectivity(double baseSel, Long cur){
		HashSet<Edge> ps = paths.get(cur);
		double sel = baseSel;
		double sum = 0;
		double temp;
		double min = 1;
		double t;
		for(Edge e : ps){
			temp = graph.getLabelFreq().get(e.getLabel()).getFrequency();
			sum += temp;
			t = ((double)temp)/graph.edgeSet().size();
			if(t < min){
				min = t;
			}
//			sel*=t;
		}
		sel *= min;
		sels.put(cur, sel);
		long mul;
		for(Edge e : ps){
			
			if(e.getDestination().equals(e.getSource())){
				continue;
			}
			temp = sel*graph.getMaxRep().get(e.getLabel());
//			temp = sel;
			if(e.getDestination().equals(cur)){
				this.computSelectivity(temp, e.getSource());
			}else{
				this.computSelectivity(temp, e.getDestination());
			}
		}
		
	}
	
	public void computSelAllNotCorrelated(double baseSel, Long cur){
		HashSet<Edge> ps = paths.get(cur);
		double sel = baseSel;
		double sum = 0;
		double temp;
		double min = 1;
		double t;
		for(Edge e : ps){
			temp = graph.getLabelFreq().get(e.getLabel()).getFrequency();
			sum += temp;
			t = ((double)temp)/graph.edgeSet().size();
			if(t < min){
				min = t;
			}
//			sel*=t;
		}
		if(min > baseSel){
			min = baseSel;
		}
		sel = min;
		sels.put(cur, sel);
		long mul;
		for(Edge e : ps){
			temp = sel*graph.getMaxRep().get(e.getLabel());
//			temp = sel;
			if(e.getDestination().equals(e.getSource())){
				continue;
			}
			if(e.getDestination().equals(cur)){
				this.computSelAllNotCorrelated(temp, e.getSource());
			}else{
				this.computSelAllNotCorrelated(temp, e.getDestination());
			}
		}
		
	}
	
	private int computeNeighbourNumber(Long node){
		int num = 0;
		Map<Long,Integer>[] qNodeTable = queryTables.getNodeMap(node);
		for (int i = 0; i < qNodeTable.length; i++) {
			num += qNodeTable[i].entrySet().size();
		}
		return num;
	}
	
	public void computSelPathNotCorrelated(double baseSel, Long cur){
		HashSet<Edge> ps = paths.get(cur);
		double sel = 1;
		double sum = 0;
		double temp;
		double min = 1;
		double t;
		
		Map<Long,Integer>[] qNodeTable = queryTables.getNodeMap(cur);
		
		for(int i = 0; i < qNodeTable.length; i++){
			for(Entry<Long, Integer> en : qNodeTable[i].entrySet()){
				temp = graph.getLabelFreq().get(en.getKey()).getFrequency();
				t = ((double)temp)/graph.edgeSet().size();
				t = Math.pow(t, en.getValue());
				sel *= t;
			}
		}
		if(sel > baseSel){
			sel = baseSel;
		}
		sels.put(cur, sel);
		long mul;
		for(Edge e : ps){
			temp = sel*graph.getMaxRep().get(e.getLabel());
//			temp = sel;
			if(e.getDestination().equals(cur)){
				this.computSelPathNotCorrelated(temp, e.getSource());
			}else{
				this.computSelPathNotCorrelated(temp, e.getDestination());
			}
		}
		
	}

	public void facotrization(ArrayList<Long> visitSeq, int ind){
		if(ind == 0){
			return;
		}
		Long child;
		ArrayList<Long> parents = new ArrayList<Long>();
		Long vertex;
		Collection<Edge> ins;
		Collection<Edge> outs;
		ParentChild pc;
		Long pLabel;
		Long cLabel;
		boolean isAdded;
		double sel = 1;
		for(int i = 0; i <= ind; i++){
			vertex = visitSeq.get(i);
			ins = graph.incomingEdgesOf(vertex);
			outs = graph.outgoingEdgesOf(vertex);
			
			if(ins.size() != 0 && outs.size() != 0){
				for(Edge in : ins){
					isAdded = false;
					for(int j = 0; j <= ind; j++){
						if(in.getSource().equals(visitSeq.get(j))){
							isAdded = true;
							break;
						}
					}
					if(isAdded){
						parents.add(in.getLabel());
					}
				}
				for(Edge out : outs){
					isAdded = false;
					for(int j = 0; j <= ind; j++){
						if(out.getDestination().equals(visitSeq.get(j))){
							isAdded = true;
							break;
						}
					}
					if(isAdded){
						cLabel = out.getLabel();
						sel *= this.computeFactorSel(parents, cLabel);
					}
				}
			}else{
				if(ins.size() == 0 && outs.size() == 0){
					continue;
				}else if(outs.size() != 0){
					for(Edge out : outs){
						isAdded = false;
						for(int j = 0; j <= ind; j++){
							if(!out.getDestination().equals(visitSeq.get(j))){
								isAdded = true;
								break;
							}
						}
						if(isAdded){
						cLabel = out.getLabel();
						sel *= ((double)graph.getLabelFreq().get(cLabel).getFrequency())/graph.edgeSet().size();
						}
					}
				}
			}
		}
		System.out.println("index:" + ind + " selectivities:" + sel );
	}
	
	public double computeFactorSel(ArrayList<Long> parents, Long child){
		double sel = 1;
		int totalParentFreq = indexing.getTotalParentFreq();
		HashMap<Long, Integer> parentFreq = indexing.getParentFreq();
		HashMap<ParentChild, Integer> pcs = indexing.getPcs();
		double denom = 1;
		double numer = 1;
		for(Long parent : parents){
			denom *= ((double)parentFreq.get(parent))/totalParentFreq;
			if(pcs.get(new ParentChild(parent, child)) != null){
			numer *= ((double)pcs.get(new ParentChild(parent, child)))/totalParentFreq;
			}
		}
		sel *= ((double)graph.getLabelFreq().get(child).getFrequency())/graph.edgeSet().size();
		sel *= numer/denom;
		return sel;
	}



	public ArrayList<Long> getVisitSeq() {
		return visitSeq;
	}



	public void setVisitSeq(ArrayList<Long> visitSeq) {
		this.visitSeq = visitSeq;
	}



	public Multigraph getQuery() {
		return query;
	}



	public void setQuery(Multigraph query) {
		this.query = query;
	}



	public BigMultigraph getGraph() {
		return graph;
	}



	public void setGraph(BigMultigraph graph) {
		this.graph = graph;
	}



	public double getSelectivity() {
		return selectivity;
	}



	public void setSelectivity(double selectivity) {
		this.selectivity = selectivity;
	}



	public Indexing getIndexing() {
		return indexing;
	}



	public void setIndexing(Indexing indexing) {
		this.indexing = indexing;
	}



	public Long getStartingNode() {
		return startingNode;
	}



	public void setStartingNode(Long startingNode) {
		this.startingNode = startingNode;
	}



	public HashMap<Long, Double> getSels() {
		return sels;
	}



	public void setSels(HashMap<Long, Double> sels) {
		this.sels = sels;
	}



	public HashMap<Long, HashSet<Edge>> getPaths() {
		return paths;
	}



	public void setPaths(HashMap<Long, HashSet<Edge>> paths) {
		this.paths = paths;
	}


	public NeighborTables getQueryTables() {
		return queryTables;
	}


	public void setQueryTables(NeighborTables queryTables) {
		this.queryTables = queryTables;
	}


	public NeighborTables getGraphTables() {
		return graphTables;
	}


	public void setGraphTables(NeighborTables graphTables) {
		this.graphTables = graphTables;
	}


	public double getPruningCost() {
		return pruningCost;
	}


	public void setPruningCost(double pruningCost) {
		this.pruningCost = pruningCost;
	}


	public double getUpdateCost() {
		return updateCost;
	}


	public void setUpdateCost(double updateCost) {
		this.updateCost = updateCost;
	}

	



	
	

	
	
}
