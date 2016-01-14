package eu.unitn.disi.db.grava.scc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import eu.unitn.disi.db.grava.graphs.BigMultigraph;
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
	}
	
	public void computeSelectivity(){
		Map<Long,Integer>[] qNodeTable = null;
		Set<Long> qSet = null;
		Map<Long, Integer> qNodeLevel;
		double selectivity;
		HashMap<Long, LabelContainer> labelFreq = ((BigMultigraph)graph).getLabelFreq();
		for(Long node : query.vertexSet()){
			selectivity = 1;
			qNodeTable = queryTable.getNodeMap(node);
			for(int i = 0; i < qNodeTable.length; i++){
				qNodeLevel = qNodeTable[i];
				qSet = qNodeLevel.keySet();
				for(Long label : qSet){
					selectivity *= (Math.pow(this.computeLabelSelectivity(labelFreq.get(label).getFrequency()),qNodeLevel.get(label))*(i+1));
				}
			}
			nodeSelectivities.put(node, selectivity);
		}
		this.sortSelectivities();
		
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

}
