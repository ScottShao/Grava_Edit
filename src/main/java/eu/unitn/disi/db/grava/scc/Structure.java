package eu.unitn.disi.db.grava.scc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import eu.unitn.disi.db.grava.graphs.Edge;

public class Structure {
	private int id;
	private int dis;
	private HashMap<Long,List> rel;
	private HashMap<Long, Long> mapping;
	private LinkedList<Long> nodesToVisit;
	private List<Edge> visitedEdges;
	
	public Structure() {
		this.id = -1;
		this.dis = -1;
		this.rel = new HashMap<Long,List>(); 
		this.mapping = new HashMap<Long, Long>();
		this.nodesToVisit = new LinkedList<Long>();
		this.visitedEdges = new ArrayList<Edge>();
	}
	
	public Structure(int id, int dis, HashMap<Long,List> rel, HashMap<Long, Long> mapping, LinkedList<Long> nodesToVisit, List<Edge> visitedEdges){
		this.id = id;
		this.dis = dis;
		this.rel = rel;
		this.mapping = mapping;
		this.nodesToVisit = nodesToVisit;
		this.visitedEdges = visitedEdges;
	}
	
	public void addNodesToVisit(Long node){
		nodesToVisit.addLast(node);
	}
	
	public HashMap<Long, Long> getMapping() {
		return mapping;
	}

	public void setMapping(HashMap<Long, Long> mapping) {
		this.mapping = mapping;
	}

	public LinkedList<Long> getNodesToVisit() {
		return nodesToVisit;
	}

	public void setNodesToVisit(LinkedList<Long> nodesToVisit) {
		this.nodesToVisit = nodesToVisit;
	}

	public List<Edge> getVisitedEdges() {
		return visitedEdges;
	}

	public void setVisitedEdges(List<Edge> visitedEdges) {
		this.visitedEdges = visitedEdges;
	}

	public void addRel(Long outNode, Long inNode ){
		List<Long> nodeList;
		if(containNode(outNode)){
			nodeList = rel.get(outNode);
			
		}else{
			nodeList = new ArrayList<Long>();
		}
		nodeList.add(inNode);
		rel.put(outNode, nodeList);
	}
	
	public boolean containNode(Long node){
		boolean flag = false;
		Iterator iter = rel.entrySet().iterator();
		while(iter.hasNext()){
			Map.Entry entry = (Map.Entry) iter.next(); 
			Long key = (Long)entry.getKey();
			if(key == node){
				flag = true;
				break;
			}
		}
		return flag;
	}
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getDis() {
		return dis;
	}

	public void setDis(int dis) {
		this.dis = dis;
	}

	public HashMap<Long, List> getRel() {
		return rel;
	}

	public void setRel(HashMap<Long, List> rel) {
		this.rel = rel;
	}
	
	

}
