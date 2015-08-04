//package eu.unitn.disi.db.grava.scc;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.Map.Entry;
//import java.util.Stack;
//
//import javafx.util.Pair;
//import sun.misc.Queue;
//import eu.unitn.disi.db.grava.exceptions.ParseException;
//import eu.unitn.disi.db.grava.graphs.BigMultigraph;
//import eu.unitn.disi.db.grava.graphs.Edge;
//import eu.unitn.disi.db.grava.graphs.LabelContainer;
//import eu.unitn.disi.db.grava.graphs.MappedEdge;
//import eu.unitn.disi.db.grava.graphs.MappedNode;
//import eu.unitn.disi.db.grava.graphs.Multigraph;
//import eu.unitn.disi.db.grava.graphs.QueryAnwser;
//import eu.unitn.disi.db.grava.utils.LabelsUltility;
//import eu.unitn.disi.db.grava.utils.StdOut;
//
//public class Query {
//	private Multigraph query;
//	private BigMultigraph graph;
//	private int threshold;
//	private int labelsCount;
//	private int droppedLabelsCount;
//	private long[] droppedLabels;
////	private Map<Long, HashSet<MappedNode>> candidate;
////	private Stack<MappedNode> nodesToVisit;
//	private HashSet<Long> visitedNodes;
//	private Stack<Pair<Long, MappedNode>> buffer;
//	private HashMap<Edge, HashSet<MappedEdge>> candidate;
//	private Stack<Long> nodesToVisit;
//	private HashSet<Edge> edgesToVisit;
// 
//	public Query(String inFile, String outFile, int threshold, boolean sort)
//			throws ParseException, IOException {
//		this.query = new BigMultigraph(inFile, outFile, sort);
//		this.labelsCount = query.edgeSet().size();
//		this.threshold = threshold;
//		this.droppedLabelsCount = this.labelsCount - threshold - 1;
//		StdOut.println(droppedLabelsCount);
//		droppedLabels = new long[droppedLabelsCount];
////		candidate = new HashMap<Long, HashSet<MappedNode>>();
////		nodesToVisit = new Stack<MappedNode>();
//		candidate = new HashMap<Edge, HashSet<MappedEdge>>();
//		nodesToVisit = new Stack<Long>();
//		edgesToVisit = new HashSet<Edge>();
//		visitedNodes = new HashSet<Long>();
//		buffer = new Stack<Pair<Long,MappedNode>>();
//		for(Edge e : query.edgeSet()){
//			edgesToVisit.add(e);
//		}
//	}
////	public void search() {
////		this.droppingLabel();
////		HashSet<Long> startingNodes = this.getStartingNodes();
////		int count = 1;
////		
////		for(Long node : startingNodes){
////			StdOut.println("Starting nodes:" + node);
////			Structure init = new Structure();
////			
////			init.setDis(0);
////			init.setId(count);
////			count ++;
////			HashMap<Long, List> tempMap = new  HashMap<Long,List>();
////			tempMap.put(node, new ArrayList<Long>());
////			init.setRel(tempMap);
////			init.addNodesToVisit(node);
////			LinkedList<Structure> sts = new LinkedList<Structure>();
////			sts.addLast(init);
////			LinkedList<Long> nodesToVisit;
////			while(!sts.isEmpty()){
////				Structure temp = sts.poll();
////				nodesToVisit = temp.getNodesToVisit();
////				while(!nodesToVisit.isEmpty()){
////					Long currentNode = nodesToVisit.poll();
////					if(temp.getMapping().get(currentNode) == null){
////						
////					}else{
////						
////					}
////				}
////			}
////			
////		}
////	}
//	public void search() {
//		this.droppingLabel();
//		HashSet<Long> startingNodes = this.getStartingNodes();
//		Collection<Long> queryNodes = query.vertexSet();
//
//		HashSet<MappedNode> mappedNodes = null;
//		Long currentNode = -1L;
//		
//		for (Long node : startingNodes){
//			visitedNodes.clear();
//			candidate.clear();
//			for(Edge e : query.edgeSet()){
//				candidate.put(e, new HashSet<MappedEdge>());
//			}
//			boolean isStartingNode = true;
//			nodesToVisit.add(node);
//			while(!nodesToVisit.isEmpty()){
//				currentNode = nodesToVisit.pop();
//				if(visitedNodes.contains(currentNode)){
//					continue;
//				}else{
//					if(isStartingNode){
//						this.match(currentNode, isStartingNode);
//						isStartingNode = false;
//					}else{
//						this.match(currentNode, isStartingNode);
//					}
//				}
//			}
//		}
////		for (Long node : startingNodes) {
////			StdOut.println("Starting Nodes: " + node);
////			visitedNodes.clear();
////			candidate.clear();
////			for (Long t : queryNodes) {
////				candidate.put(t, new HashSet<MappedNode>());
////			}
////			boolean flag = true;
////			nodesToVisit.add(new MappedNode(node, -1, 0));
////			while(!nodesToVisit.isEmpty()){
////				mn = nodesToVisit.pop();
////				if(visitedNodes.contains(mn.getNodeID())){
////					continue;
////				}
////				if(flag){
////					this.match(mn, flag);
////					flag =false;
////				}else{
////					this.match(mn, flag);
////				}
////			}
////			for (Edge e : query.edgeSet()){
////				Long src = e.getSource();
////				Long des = e.getDestination();
////				ArrayList<Edge> edges = new ArrayList<Edge>();
////				HashSet<MappedNode> mappedSrc = candidate.get(src);
////				HashSet<MappedNode> mappedDes = candidate.get(des);
////				StdOut.println("\n");
////				StdOut.println("query:" + e.toString());
////				//for(MappedNode mnSrc : mappedSrc){
////					for(MappedNode mnDes : mappedDes){
////						
////						if(src == mnDes.getQuerySrc()){
//////							edges.add(mnDes.getMappedEdges());
////							StdOut.println(mnDes.getMappedEdges().toString());
////						}
////					}
////				//}
////				
////			}
////			
////		}
//		
////		for(Entry entry : candidate.entrySet()){
////			System.out.println("Query:" + (Long)entry.getKey());
////			HashSet<MappedNode> set = (HashSet<MappedNode>) entry.getValue();
////			System.out.println("Mapped nodes:");
////			for(MappedNode node : set){
////				node.print();
////			}
////		}
////		HashMap<Edge, ArrayList<Edge>> mappedEdges = new HashMap<Edge, ArrayList<Edge>>();
////		ArrayList<QueryAnwser> answers = new ArrayList<QueryAnwser>();
//		
//	}
//
//	private void match(Long currentNode, boolean isStartingNode) {
////		if(currentNode.getDist() > this.threshold){
////			while(buffer.peek() != nodesToVisit.peek().getNodeID()){
////				buffer.pop();
////			}
////			return;
////		}
////		if(currentNode.getFinishedEdges().size() == query.edgeSet().size()){
////			return;
////		}
//		
////		HashSet<MappedNode> nodes = null;
//		HashSet<MappedEdge> edges = null;
//		MappedEdge me = null;
//		visitedNodes.add(currentNode);
//		Collection<Edge> outEdges = graph.outgoingEdgesOf(currentNode);
//		for(Edge e : edgesToVisit){
//			edges = candidate.get(e);
//		}
////		MappedNode mn = null;
////		HashSet<Long> visited = new HashSet<Long>();
////		long[][] nodeEdge = graph.outgoingArrayEdgesOf(currentNode.getNodeID());
////		visitedNodes.add(currentNode.getNodeID());
////		boolean flag = false;
//		for (Edge e : query.edgeSet()) {
//			nodes = candidate.get(e.getSource());
//			if(currentNode.containEdge(e)){
//				continue;
//			}
//			
//			if(!visited.contains(e.getSource())){
//				visited.add(e.getSource());
//				if(isStartingNode){
//					nodes.add(new MappedNode(currentNode.getNodeID(), -1, 0));
//					candidate.put(e.getSource(), nodes);
//					//buffer.push(new Pair<Long, MappedNode>(e.getSource(), currentNode));
//					
//				}
//			}
//			
//			for (int i = 0; i < nodeEdge.length; i++) {
////				System.out.println(e.toString());
////				StdOut.println(e.getDestination());
//				nodes = candidate.get(e.getDestination());
//				if (nodeEdge[i][2] == e.getLabel()) {
//					mn = new MappedNode(nodeEdge[i][1], nodeEdge[i][0], currentNode.getDist()); 
//					mn.setMappedEdge(new Edge(nodeEdge[i][0], nodeEdge[i][1], e.getLabel()));
//					mn.setQuerySrc(e.getSource());
//					mn.addFinishedEdge(e);
//					nodes.add(mn);
//					
//				}else{
//					if(currentNode.getDist()+1 > this.threshold){
//						continue;
//					}
//					mn = new MappedNode(nodeEdge[i][1], nodeEdge[i][0], currentNode.getDist()+1);
//					mn.setQuerySrc(e.getSource());
//					mn.setMappedEdge(new Edge(nodeEdge[i][0], nodeEdge[i][1], e.getLabel()));
//					mn.addFinishedEdge(e);
//					nodes.add(mn);
//				}
//				candidate.put(e.getDestination(), nodes);
//				//mn.print();
//				//buffer.push(new Pair<Long, MappedNode>(e.getDestination(), mn));
//				nodesToVisit.push(mn);
//				
//			}
//		}
//	}
//
//	private void droppingLabel() {
//		HashMap<Long, LabelContainer> labelFreq = graph.getLabelFreq();
//		if (droppedLabelsCount > 0) {
//			droppedLabels = LabelsUltility.droppingLabels(labelFreq,
//					droppedLabelsCount);
//			for (int i = 0; i < droppedLabels.length; i++) {
//				StdOut.println( "dropped label:" + droppedLabels[i]);
//			}
//		}
//	}
//
//	private boolean isDropped(long label) {
//		boolean flag = false;
//		for (int i = 0; i < droppedLabels.length; i++) {
//			//StdOut.println("dropped labels "+ droppedLabels[i]);
//			if (label == droppedLabels[i]) {
//				flag = true;
//				break;
//			}
//		}
//		return flag;
//	}
//
//	private HashSet<Long> getStartingNodes() {
//		HashSet<Long> startingNodes = null;
//		HashMap<Long, LabelContainer> labelFreq = graph.getLabelFreq();
//		for (Entry<Long, LabelContainer> temp : labelFreq.entrySet()) {
//			if (!isDropped(temp.getKey())) {
//				startingNodes = LabelsUltility.mergeNodes(startingNodes, temp
//						.getValue().getNodes());
//			}
//		}
//		return startingNodes;
//	}
//
//	public Multigraph getQuery() {
//		return query;
//	}
//
//	public void setQuery(Multigraph query) {
//		this.query = query;
//	}
//
//	public BigMultigraph getGraph() {
//		return graph;
//	}
//
//	public void setGraph(BigMultigraph graph) {
//		this.graph = graph;
//	}
//
//	public int getThreshold() {
//		return threshold;
//	}
//
//	public void setThreshold(int threshold) {
//		this.threshold = threshold;
//	}
//
//	public int getLabelsCount() {
//		return labelsCount;
//	}
//
//	public void setLabelsCount(int labelsCount) {
//		this.labelsCount = labelsCount;
//	}
//
//	public int getDroppedLabelsCount() {
//		return droppedLabelsCount;
//	}
//
//	public void setDroppedLabelsCount(int droppedLabelsCount) {
//		this.droppedLabelsCount = droppedLabelsCount;
//	}
//
//	public long[] getDroppedLabels() {
//		return droppedLabels;
//	}
//
//	public void setDroppedLabels(long[] droppedLabels) {
//		this.droppedLabels = droppedLabels;
//	}
//
//	public static void main(String[] args) throws ParseException, IOException {
//		BigMultigraph G = new BigMultigraph("graph10Nodes.txt",
//				"graph10Nodes.txt", true);
//		Query q = new Query("query.txt", "query.txt", 0, true);
//		q.setGraph(G);
//		q.search();
//
//	}
//
//}
