/*
 * Copyright (C) 2013 Davide Mottin <mottin@disi.unitn.eu>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package eu.unitn.disi.db.exemplar.core.algorithms;

import eu.unitn.disi.db.command.algorithmic.Algorithm;
import eu.unitn.disi.db.command.algorithmic.AlgorithmInput;
import eu.unitn.disi.db.command.algorithmic.AlgorithmOutput;
import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.mutilities.StopWatch;
import eu.unitn.disi.db.grava.exceptions.DataException;
import eu.unitn.disi.db.grava.graphs.BaseMultigraph;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.LabelContainer;
import eu.unitn.disi.db.grava.graphs.MappedNode;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.graphs.Path;
import eu.unitn.disi.db.grava.graphs.PathNeighbor;
import eu.unitn.disi.db.grava.utils.Utilities;
import eu.unitn.disi.db.grava.vectorization.NeighborTables;
import eu.unitn.disi.db.grava.vectorization.PathNeighborTables;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Pruning algorithm using neighborhood information for each node.
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class PruningAlgorithm extends Algorithm {
    @AlgorithmInput
    private Multigraph query;
    @AlgorithmInput
    private Multigraph graph;
    @AlgorithmInput
    private NeighborTables graphTables;
    @AlgorithmInput
    private NeighborTables queryTables;
    @AlgorithmInput
    private PathNeighborTables graphPathTables;
    @AlgorithmInput
    private PathNeighborTables queryPathTables;
    @AlgorithmInput
    private Long startingNode;
    @AlgorithmInput
    private boolean isBinary = false;
    @AlgorithmInput
    private int threshold;
    private HashMap<Long, Double> prefixSelectivities;
    private int edgeNum;
    private HashMap<Long, LabelContainer> labelFreq;
    private HashMap<Long, Double> nodeSelectivities;
    private HashMap<Long, Integer> neighborLabels;
    private HashMap<Long, Integer> candidates;
    private int bsCount;
    private int cmpNeighCount;
    private int uptCount;
    private ArrayList<Long> visitSeq;
    
    @AlgorithmOutput
    private Map<Long,Set<MappedNode>> queryGraphMapping;
    @AlgorithmOutput
    private int numberOfComparison;
    private HashMap<Long, HashSet<Edge>> paths;

    @Override
    public void compute()
            throws AlgorithmExecutionException
    {
        //Initialize the output.
        bsCount  = 0;
        cmpNeighCount = 0;
        uptCount = 0;
    	queryGraphMapping = new HashMap<>();
        prefixSelectivities = new HashMap<Long, Double>();
        edgeNum = graph.edgeSet().size();
        //Map<Long,Integer> nodeFrequency;
        Map<Long,Integer> labelFrequency = new HashMap<>();
        Map<Long, Set<MappedNode>> candidateNextLevel = new HashMap<>();
        labelFreq = ((BigMultigraph)graph).getLabelFreq();
        nodeSelectivities = new HashMap<Long, Double>();
        neighborLabels = new HashMap<Long, Integer>();
        visitSeq = new ArrayList<Long>();
        paths = new HashMap<Long, HashSet<Edge>>();
        candidates = new HashMap<Long, Integer>();
        //Long label;
        Long candidate, currentQueryNode;
        MappedNode graphCandidate;
        numberOfComparison = 0;
        boolean first = true;
        Integer frequency;
        LinkedList<Long> queryNodeToVisit = new LinkedList<>();
        List<MappedNode> nodesToVisit;
        Collection<Edge> graphEdges = graph.edgeSet();
        Collection<Edge> queryEdges;

        Map<Long, List<Long>> inQueryEdges;
        Map<Long, List<Long>> outQueryEdges;
        Collection<Long> queryNodes = query.vertexSet();
        Set<MappedNode> mappedNodes;
        Set<Long> visitedQueryNodes = new HashSet<>();
        int i;
        this.computeSelectivity();
        for (Edge e : graphEdges) {
            frequency = labelFrequency.get(e.getLabel());
            if (frequency == null) {
                frequency = 0;
            }
            frequency++;
            labelFrequency.put(e.getLabel(), frequency);
        }
        //Just to try - Candidate is the first
//        startingNode = 5048L;
        if (startingNode == null) {
            candidate = queryNodes.iterator().next();
        } else {
            candidate = startingNode;
        }
        queryNodeToVisit.add(candidate);

        //Initialize the candidate qnode -> gnode
        for (Long node : queryNodes) {
           candidateNextLevel.put(node, new HashSet<MappedNode>());
        }
        prefixSelectivities.put(startingNode, (double) 1);
        candidateNextLevel.put(candidate, Utilities.nodesToMappedNodes(graph.vertexSet()));
        try {
            while (!queryNodeToVisit.isEmpty()) {
                currentQueryNode = queryNodeToVisit.poll();
                visitSeq.add(currentQueryNode);
                mappedNodes = queryGraphMapping.get(currentQueryNode);
                //Compute the valid edges to explore and update the nodes to visit
                inQueryEdges = computeAdjacentNodes(currentQueryNode, visitedQueryNodes, queryNodeToVisit, true);
                outQueryEdges = computeAdjacentNodes(currentQueryNode, visitedQueryNodes, queryNodeToVisit, false);

                if (candidateNextLevel.containsKey(currentQueryNode)) {
                	
                	nodesToVisit = new ArrayList<MappedNode>();
                    nodesToVisit.addAll(candidateNextLevel.get(currentQueryNode));
                    assert mappedNodes == null : String.format("The current query node %d, has already been explored", currentQueryNode);
                    mappedNodes = new HashSet<>();
                    
                    //countNodes = 0;
                    //We should check if ALL the query nodes matches and then add the node
                    for (i = 0; i < nodesToVisit.size(); i++) {
                        graphCandidate = nodesToVisit.get(i);
//                        if (this.matchesWithPathNeighbor(graphCandidate, currentQueryNode)) {
                        	
                        if (this.matches(graphCandidate, currentQueryNode)) {
                            numberOfComparison++;
                            mappedNodes.add(graphCandidate);
                            //check if the outgoing-incoming edges matches, if yes add to the next level
                            mapNodes(graphCandidate, graph.incomingEdgesOf(graphCandidate.getNodeID()), inQueryEdges, candidateNextLevel, true);
                            mapNodes(graphCandidate, graph.outgoingEdgesOf(graphCandidate.getNodeID()), outQueryEdges, candidateNextLevel, false);
                            bsCount += 2;
                        }
                        else{
//                        	System.out.println("fail");
                        }
                    }
                    queryGraphMapping.put(currentQueryNode, mappedNodes);
                    candidates.put(currentQueryNode, mappedNodes.size());
                    if(currentQueryNode.equals(48497476877148L)){
	                    for(MappedNode tt : mappedNodes){
	                    	System.out.println("candidate:" + tt.getNodeID());
	                    }
                    }
//                    System.out.println("node:" + currentQueryNode + " candidate number:" + mappedNodes.size());
                    //add the out edges to the visited ones
                    visitedQueryNodes.add(currentQueryNode);
                } else { //No map is possible anymore
                    break;
                }
            }
//            this.print();
//            debug("The number of comparison is %d", numberOfComparison);
        } catch (DataException ex) {
            //fatal("Some problems with the data occurred", ex);
            throw new AlgorithmExecutionException("Some problem with the data occurrred", ex);
        } catch (Exception ex) {
            //fatal("Some problem occurred", ex);
            ex.printStackTrace();
        	throw new AlgorithmExecutionException("Some other problem occurred", ex);
        }
//        this.computeTimeCost();
        //Choose the node with the least frequency.
    }

    public PathNeighborTables getGraphPathTables() {
		return graphPathTables;
	}

	public void setGraphPathTables(PathNeighborTables graphPathTables) {
		this.graphPathTables = graphPathTables;
	}

	public PathNeighborTables getQueryPathTables() {
		return queryPathTables;
	}

	public void setQueryPathTables(PathNeighborTables queryPathTables) {
		this.queryPathTables = queryPathTables;
	}
	
	private void print(){
		for(Entry<Long, HashSet<Edge>> en : paths.entrySet()){
			System.out.println("node : " + en.getKey() + "edges:");
			for(Edge e : en.getValue()){
				System.out.println(e);
			}
		}
	}
	
	private void mapNodes(MappedNode currentNode, Collection<Edge> graphEdges, Map<Long, List<Long>> queryEdges, Map<Long, Set<MappedNode>> nextLevel, boolean incoming) {
        MappedNode nodeToAdd = null;
        List<Long> labeledNodes;
        List<Long> omniNodes;
        Long nodeID;
        int i;
        boolean canHaveMoreDif = true;
        omniNodes = queryEdges.get(0L);
        uptCount ++;
        if(currentNode.getDist() == threshold){
        	canHaveMoreDif = false;
        }
        for (Edge gEdge : graphEdges) {
            nodeID = incoming? gEdge.getSource() : gEdge.getDestination();
            labeledNodes = queryEdges.get(gEdge.getLabel());
            if(omniNodes != null){
            	if(labeledNodes == null){
            		labeledNodes = new ArrayList<Long>();
            	}
            	for(Long omniNode: omniNodes){
            		labeledNodes.add(omniNode);
            	}
            }
            if (labeledNodes != null) {
            	nodeToAdd = new MappedNode(nodeID, gEdge, currentNode.getDist(), !incoming, false);
                for (i = 0; i < labeledNodes.size(); i++) {
                    nextLevel.get(labeledNodes.get(i)).add(nodeToAdd);
                }
                if(canHaveMoreDif){
            		for(Entry<Long, List<Long>> entry : queryEdges.entrySet()){
            			for(Long oneNode : entry.getValue()){
            				if(!labeledNodes.contains(oneNode)){
            					nextLevel.get(oneNode).add(new MappedNode(nodeID, gEdge, currentNode.getDist()+1, !incoming, true));
            				}
            			}
            		}
            	}
            }else{
            	if(canHaveMoreDif){
            		for(Entry<Long, List<Long>> entry : queryEdges.entrySet()){
            			for(Long oneNode : entry.getValue()){
            				nextLevel.get(oneNode).add(new MappedNode(nodeID, gEdge, currentNode.getDist()+1, !incoming, true));
            			}
            		}
            	}
            }
        }
    }

    //label -> nextNodes
    private Map<Long, List<Long>> computeAdjacentNodes(long node, Set<Long> visitedQueryNodes, List<Long> queryNodeToVisit, boolean incoming)
    {
        Collection<Edge> queryEdges =
                incoming? query.incomingEdgesOf(node) : query.outgoingEdgesOf(node);
        
        List<Long> nodes;
        Map<Long, List<Long>> outMapping = new HashMap<>();
        Set<Long> toVisit = new HashSet<>();
        Long nodeToAdd;
        double preSel = prefixSelectivities.get(node);
        double sel;
        HashSet<Edge> ps;
        if(paths.containsKey(node)){
        	ps = paths.get(node);
        }else{
        	ps = new HashSet<Edge>();
        }
        for (Edge edge : queryEdges) {
            nodes = outMapping.get(edge.getLabel());
            if (nodes == null) {
                nodes = new ArrayList<>();
            }
            nodeToAdd = incoming? edge.getSource() : edge.getDestination();
            if (!visitedQueryNodes.contains(nodeToAdd)) {
                nodes.add(nodeToAdd);
                toVisit.add(nodeToAdd);
                ps.add(edge);
            }
            
            if(!prefixSelectivities.containsKey(nodeToAdd)){
            	prefixSelectivities.put(nodeToAdd, preSel*this.computeSel(labelFreq.get(edge.getLabel()).getFrequency()));
            }else{
            	sel = prefixSelectivities.get(nodeToAdd);
            	prefixSelectivities.put(nodeToAdd, sel + preSel*this.computeSel(labelFreq.get(edge.getLabel()).getFrequency()));
            }
            
            outMapping.put(edge.getLabel(), nodes);
        }
        paths.put(node, ps);
        queryNodeToVisit.addAll(toVisit);
        return outMapping;
    }
    
    private boolean matchesWithPathNeighbor(MappedNode mappedGNode, long qNode) throws DataException {
        Map<PathNeighbor,Integer>[] gNodeTable = graphPathTables.getNodeMap(mappedGNode.getNodeID());
        Map<PathNeighbor,Integer>[] qNodeTable = queryPathTables.getNodeMap(qNode);
        Map<PathNeighbor, Integer> qNodeLevel, gNodeLevel;
        Set<PathNeighbor> qSet;
        
//        if(mappedGNode.getNodeID() == 765448){
//        	System.out.println();
//        }
        for (int i = 0; i < qNodeTable.length && i < gNodeTable.length; i++) {
            qNodeLevel = qNodeTable[i];
            gNodeLevel = gNodeTable[i];
            qSet = qNodeLevel.keySet();
            int dif = 0;
            
            for (PathNeighbor pn : qSet) {
//            	if(label.equals(0L)){
//            		continue;
//            	}
                if (gNodeLevel.containsKey(pn)) {
                	int count = qNodeLevel.get(pn) - gNodeLevel.get(pn); 
                    if (!isBinary && count > threshold - dif) {
                        return false;
                    }else{
                    	dif += count;
                    }
                } else {

                    dif += qNodeLevel.get(pn);
                    if(dif > threshold){
                    	return false;
                    }
                }
            }
        }
        return true;
    }
    
    public void computeTimeCost(){
    	int Vg = graph.vertexSet().size();
    	int Vq = graph.vertexSet().size();
    	long Dg = ((BigMultigraph)graph).getMaxDegree();
    	long Dq = ((BigMultigraph)query).getMaxDegree();
    	double pt = 0;
    	double ut = 0;
    	double st = Dg;
    	double sel = 0;
    	double tempPt;
    	double tempUt;
    	for(long node : query.vertexSet()){
    		sel = prefixSelectivities.get(node)*nodeSelectivities.get(node);
    		tempPt = Vg*prefixSelectivities.get(node);
    		pt += tempPt*neighborLabels.get(node);
    		tempUt = Vg*sel;
    		ut += Dg*Dq*tempUt;
//    		System.out.println("node: " + node + " before pruning sel:" + tempPt + " after pruning sel:" + tempUt);
    		st *= Vg*sel;
//    		System.out.println(st);
    	}
    	
//    	System.out.println("pruning time + update time is " + (pt+ut));
//    	System.out.println("search time is " + st);
    }
    
    public ArrayList<Long> getVisitSeq() {
		return visitSeq;
	}

	public void setVisitSeq(ArrayList<Long> visitSeq) {
		this.visitSeq = visitSeq;
	}

	private void computeSelectivity(){
		Map<Long,Integer>[] qNodeTable = null;
		Set<Long> qSet = null;
		Map<Long, Integer> qNodeLevel;
		double selectivity;
		int count;
		for(Long node : query.vertexSet()){
			selectivity = 1;
			count = 0;
			qNodeTable = queryTables.getNodeMap(node);
			for(int i = 0; i < qNodeTable.length; i++){
				qNodeLevel = qNodeTable[i];
				qSet = qNodeLevel.keySet();
				count += qSet.size();
				for(Long label : qSet){
					selectivity *= (Math.pow(this.computeSel(labelFreq.get(label).getFrequency()),qNodeLevel.get(label))*(i+1));
				}
			}
			neighborLabels.put(node, count);
			nodeSelectivities.put(node, selectivity);
		}
		
	}
    
    private boolean matches(MappedNode mappedGNode, long qNode) throws DataException {

        Map<Long,Integer>[] gNodeTable = graphTables.getNodeMap(mappedGNode.getNodeID());
        Map<Long,Integer>[] qNodeTable = queryTables.getNodeMap(qNode);
        Map<Long, Integer> qNodeLevel, gNodeLevel;
        Set<Long> qSet;
        int dif = 0;
        cmpNeighCount ++;
        for (int i = 0; i < qNodeTable.length && i < gNodeTable.length; i++) {
            qNodeLevel = qNodeTable[i];
            gNodeLevel = gNodeTable[i];
            qSet = qNodeLevel.keySet();

            for (Long label : qSet) {
            	if(label.equals(0L)){
            		continue;
            	}
                if (gNodeLevel.containsKey(label)) {
                	int count = qNodeLevel.get(label) - gNodeLevel.get(label);
                	
                    if (!isBinary && count > threshold - dif) {
                        return false;
                    }else{
                    	if(count > 0){
                    		dif += count;
                    	}
                    }
                } else {
                    dif += qNodeLevel.get(label);
                    if(dif > threshold){
                    	return false;
                    }
                }
            }
        }
        return true;
    }


    /**
     *
     * @return a pruned graph exploiting the Query To Graph Map
     * @throws AlgorithmExecutionException
     */
    public Multigraph pruneGraph() throws AlgorithmExecutionException {
        StopWatch watch = new StopWatch();
        watch.start();

        Collection<Long> queryNodes = query.vertexSet();
        Collection<Edge> graphEdges = graph.edgeSet();

        Set<MappedNode> goodNodes = new HashSet<>();

        Multigraph restricted = new BaseMultigraph(graph.edgeSet().size());

        Edge tmpEdge;
        int removed = 0;

        for (Long node : queryNodes) {

            if (!queryGraphMapping.containsKey(node) || queryGraphMapping.get(node).isEmpty()) {
                //TODO Long should be converted to redable
                throw new AlgorithmExecutionException("Query tables do not contain maps for the node " + node);
            }

            goodNodes.addAll(queryGraphMapping.get(node));
        }

        for (Iterator<Edge> it = graphEdges.iterator(); it.hasNext();) {
            tmpEdge = it.next();
            if (goodNodes.contains(tmpEdge.getDestination()) && goodNodes.contains(tmpEdge.getSource())) {
                restricted.addVertex(tmpEdge.getSource());
                restricted.addVertex(tmpEdge.getDestination());
                restricted.addEdge(tmpEdge);
            } else {
                removed++;
            }
        }
        debug("kept %d, removed %d over %d edges non mapping edges in %dms", restricted.edgeSet().size(), removed, graphEdges.size(), watch.getElapsedTimeMillis());

        return restricted;
    }


    public void setQuery(Multigraph query) {
        this.query = query;
    }

    public void setGraph(Multigraph graph) {
        this.graph = graph;
    }

    public void setGraphTables(NeighborTables graphTables) {
        this.graphTables = graphTables;
    }

    public void setQueryTables(NeighborTables queryTables) {
        this.queryTables = queryTables;
    }

    public Map<Long, Set<MappedNode>> getQueryGraphMapping() {
        return queryGraphMapping;
    }

    public void setStartingNode(Long node) {
        this.startingNode = node;
    }

    public int getNumberOfComparison() {
        return numberOfComparison;
    }

    public boolean isBinary() {
        return isBinary;
    }

    /**
     * When Binary is set to true the tables will be used to check for presence of arcs but not number of archs
     * this is used for simulation instead of isomorphism
     * @param isBinary
     */
    public void setBinary(boolean isBinary) {
        this.isBinary = isBinary;
    }

	public int getThreshold() {
		return threshold;
	}

	public void setThreshold(int threshold) {
		this.threshold = threshold;
	}

    private double computeSel(int freq){
    	return ((double)freq)/edgeNum;
    }

	public int getBsCount() {
		return bsCount;
	}

	public void setBsCount(int bsCount) {
		this.bsCount = bsCount;
	}

	public int getCmpNeighCount() {
		return cmpNeighCount;
	}

	public void setCmpNeighCount(int cmpNeighCount) {
		this.cmpNeighCount = cmpNeighCount;
	}

	public int getUptCount() {
		return uptCount;
	}

	public void setUptCount(int uptCount) {
		this.uptCount = uptCount;
	}

	public HashMap<Long, HashSet<Edge>> getPaths() {
		return paths;
	}

	public void setPaths(HashMap<Long, HashSet<Edge>> paths) {
		this.paths = paths;
	}

	public HashMap<Long, Integer> getCandidates() {
		return candidates;
	}

	public void setCandidates(HashMap<Long, Integer> candidates) {
		this.candidates = candidates;
	}
    
    

}
