package eu.unitn.disi.db.grava.scc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.LabelContainer;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.vectorization.NeighborTables;

public class NextQueryVertexes {
    private Multigraph query;
    private Multigraph graph;
    private HashMap<Long, Double> nodeSelectivities;
    private NeighborTables queryTable;
    private int edgeNum;
    private ArrayList<Long> sortedNodes;
    private int index;
    private HashMap<Long, LabelContainer> labelFreq;
    
	public NextQueryVertexes() {
		query = null;
		graph = null;
	}
	
	public NextQueryVertexes(Multigraph graph, Multigraph query, NeighborTables queryTable){
		this.query = query;
		this.graph = graph;
		this.queryTable = queryTable;
		this.edgeNum = graph.edgeSet().size();
		this.sortedNodes = new ArrayList<Long>();
		this.index = 0;
		this.nodeSelectivities = new HashMap<Long, Double>();
		this.labelFreq = ((BigMultigraph)graph).getLabelFreq();
	}
	
	public void computeSelectivity(){
		Map<Long,Integer>[] qNodeTable = null;
		Set<Long> qSet = null;
		Map<Long, Integer> qNodeLevel;
		double selectivity;
		int tempFreq;
		for(Long node : query.vertexSet()){
			selectivity = 1;
			qNodeTable = queryTable.getNodeMap(node);
			nodeSelectivities.put(node, graph.outDegreeOf(node) + graph.inDegreeOf(node)+computeExSel(qNodeTable));
//			nodeSelectivities.put(node, computeSelPathNotCor(node, qNodeTable.length));
		}
		this.sortSelectivities();
		
	}
	
	private double computeExSel(Map<Long,Integer>[] qNodeTable){
		double sel = 0;
		Set<Long> qSet = null;
		Map<Long, Integer> qNodeLevel;
		for(int i = 0; i < qNodeTable.length; i++){
			qNodeLevel = qNodeTable[i];
			qSet = qNodeLevel.keySet();
			for(Long label : qSet){
				sel += qNodeLevel.get(label)*labelFreq.get(label).getFrequency()/(i+1);
			}
		}
		return sel;
	}
	
	private double computeSel(Map<Long,Integer>[] qNodeTable){
		double re = 1;
		double sel = 0;
		Set<Long> qSet = null;
		Map<Long, Integer> qNodeLevel;
		
		for(int i = 0; i < qNodeTable.length; i++){
			qNodeLevel = qNodeTable[i];
			qSet = qNodeLevel.keySet();
			for(Long label : qSet){
				sel = this.computeLabelSelectivity(labelFreq.get(label).getFrequency());
				re *= sel;
			}
		}
		return re;
	}
	
	private double computeSelAdjNotCor(Map<Long,Integer>[] qNodeTable){
		double re = 1;
		double sel = 0;
		Set<Long> qSet = null;
		Map<Long, Integer> qNodeLevel;
		double min;
		for(int i = 0; i < qNodeTable.length; i++){
			qNodeLevel = qNodeTable[i];
			qSet = qNodeLevel.keySet();
			min = 1;
			for(Long label : qSet){
				sel = this.computeLabelSelectivity(labelFreq.get(label).getFrequency());
				if(sel < min){
					min = sel;
				}
			}
			re *= min;
		}
		return re;
	}
	
	private double computeSelPathNotCor(Long queryNode, int depth){
		return dfs(1, queryNode, depth);
	}
	
	private double dfs(double baseSel, Long queryNode, int depth){
		if(depth == 0){
			return baseSel;
		}
		double tempSel = 0;
		double sel = 1;
		Collection<Edge> edges = query.outgoingEdgesOf(queryNode);
		ArrayList<Long> nextNodes = new ArrayList<Long>();
		Long nextNode = null;
		for(Edge edge : edges){
			tempSel = this.computeLabelSelectivity(labelFreq.get(edge.getLabel()).getFrequency());
//			System.out.println(edge.getLabel() + " " + tempSel);
			sel *= tempSel;
			nextNode = queryNode.equals(edge.getSource())?edge.getDestination():edge.getSource();
			nextNodes.add(nextNode);
		}
		
		edges = query.incomingEdgesOf(queryNode);
		for(Edge edge : edges){
			tempSel = this.computeLabelSelectivity(labelFreq.get(edge.getLabel()).getFrequency());
//			System.out.println(edge.getLabel() + " " + tempSel);
			sel *= tempSel;
			nextNode = queryNode.equals(edge.getSource())?edge.getDestination():edge.getSource();
			nextNodes.add(nextNode);
		}
		if(sel < baseSel){
			baseSel = sel;
		}
		
		if(depth == 1){
			return baseSel;
		}else{
			for(Long next : nextNodes){
				baseSel = dfs(baseSel, next, depth-1);
			}
			return baseSel;
		}
		
		
	}
	
	private double computeLabelSelectivity(int frequency){
		return (double)frequency/edgeNum;
	}
	
	private void sortSelectivities(){
		List<Map.Entry<Long,Double>> list = new ArrayList<Map.Entry<Long,Double>>(nodeSelectivities.entrySet());
		Collections.sort(list,new Comparator<Map.Entry<Long,Double>>() {
            
            public int compare(Entry<Long, Double> o1,
                    Entry<Long, Double> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
            
        });
		
		for(Map.Entry<Long,Double> mapping:list){ 
            sortedNodes.add(mapping.getKey());
//            System.out.println(mapping.getKey() + " " + mapping.getValue());
       } 
	}
	
	public Long getNextVertexes(){
		Long vertex = sortedNodes.get(index);
		index++;
		return vertex;
	}

	public HashMap<Long, Double> getNodeSelectivities() {
		return nodeSelectivities;
	}

	public void setNodeSelectivities(HashMap<Long, Double> nodeSelectivities) {
		this.nodeSelectivities = nodeSelectivities;
	}
	
}
