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
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.MappedNode;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.utils.Utilities;
import eu.unitn.disi.db.grava.vectorization.NeighborTables;

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
    private Long startingNode;
    @AlgorithmInput
    private boolean isBinary = false;
    @AlgorithmInput
    private int threshold;

    @AlgorithmOutput
    private Map<Long,Set<MappedNode>> queryGraphMapping;
    @AlgorithmOutput
    private int numberOfComparison;

    @Override
    public void compute()
            throws AlgorithmExecutionException
    {
        //Initialize the output.
        queryGraphMapping = new HashMap<>();

        //Map<Long,Integer> nodeFrequency;
        Map<Long,Integer> labelFrequency = new HashMap<>();
        Map<Long, Set<MappedNode>> candidateNextLevel = new HashMap<>();

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
        
        candidateNextLevel.put(candidate, Utilities.nodesToMappedNodes(graph.vertexSet()));
        try {
            while (!queryNodeToVisit.isEmpty()) {
                currentQueryNode = queryNodeToVisit.poll();
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
//                        System.out.println("matching " + currentQueryNode + " with " + graphCandidate.getNodeID());
                        if (matches(graphCandidate, currentQueryNode)) {
//                        	System.out.println("success");
                            numberOfComparison++;
                            mappedNodes.add(graphCandidate);
                            //check if the outgoing-incoming edges matches, if yes add to the next level
                            mapNodes(graphCandidate, graph.incomingEdgesOf(graphCandidate.getNodeID()), inQueryEdges, candidateNextLevel, true);
                            mapNodes(graphCandidate, graph.outgoingEdgesOf(graphCandidate.getNodeID()), outQueryEdges, candidateNextLevel, false);
                        }
                        else{
//                        	System.out.println("fail");
                        }
                    }
                    queryGraphMapping.put(currentQueryNode, mappedNodes);
                    //add the out edges to the visited ones
                    visitedQueryNodes.add(currentQueryNode);
                } else { //No map is possible anymore
                    break;
                }
            }
            debug("The number of comparison is %d", numberOfComparison);
        } catch (DataException ex) {
            //fatal("Some problems with the data occurred", ex);
            throw new AlgorithmExecutionException("Some problem with the data occurrred", ex);
        } catch (Exception ex) {
            //fatal("Some problem occurred", ex);
            ex.printStackTrace();
        	throw new AlgorithmExecutionException("Some other problem occurred", ex);
        }
        //Choose the node with the least frequency.
    }

    private void mapNodes(MappedNode currentNode, Collection<Edge> graphEdges, Map<Long, List<Long>> queryEdges, Map<Long, Set<MappedNode>> nextLevel, boolean incoming) {
        MappedNode nodeToAdd = null;
        List<Long> labeledNodes;
        List<Long> omniNodes;
        Long nodeID;
        int i;
        boolean canHaveMoreDif = true;
        omniNodes = queryEdges.get(0L);
       
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

        for (Edge edge : queryEdges) {
            nodes = outMapping.get(edge.getLabel());
            if (nodes == null) {
                nodes = new ArrayList<>();
            }
            nodeToAdd = incoming? edge.getSource() : edge.getDestination();
            if (!visitedQueryNodes.contains(nodeToAdd)) {
                nodes.add(nodeToAdd);
                toVisit.add(nodeToAdd);
            }
            outMapping.put(edge.getLabel(), nodes);
        }
        queryNodeToVisit.addAll(toVisit);
        return outMapping;
    }

    private boolean matches(MappedNode mappedGNode, long qNode) throws DataException {
        Map<Long,Integer>[] gNodeTable = graphTables.getNodeMap(mappedGNode.getNodeID());
        Map<Long,Integer>[] qNodeTable = queryTables.getNodeMap(qNode);
        Map<Long, Integer> qNodeLevel, gNodeLevel;
        Set<Long> qSet;
//        int dif = mappedGNode.getDist();
        int dif = 0;
        //D:/*
//        if (gNode == 84748654765648L) {
//            for (int i = 0; i < gNodeTable.length; i++) {
//                error(Utilities.mapToString(gNodeTable[i]));
//            }
//            for (int i = 0; i < qNodeTable.length; i++) {
//                error(Utilities.mapToString(qNodeTable[i]));
//            }
//        }
        //D:*/

        for (int i = 0; i < qNodeTable.length && i < gNodeTable.length; i++) {
            qNodeLevel = qNodeTable[i];
            gNodeLevel = gNodeTable[i];
            qSet = qNodeLevel.keySet();

            for (Long label : qSet) {
            	if(label.equals(0L)){
//            		System.out.println("wang neng bian");
            		continue;
            	}
                if (gNodeLevel.containsKey(label)) {
                	int count = qNodeLevel.get(label) - gNodeLevel.get(label); 
                    if (!isBinary && count > threshold - dif) {
//        //D:/*
//                        if (gNode == 84748654765648L) {
//                            try {
//                                error("Less information for label %s, queryNode %s at distance %d", FreebaseConstants.getPropertyMid(label), FreebaseConstants.convertLongToMid(qNode), i);
//                            } catch (IOException ex) {
//                                Logger.getLogger(PruningAlgorithm.class.getName()).log(Level.SEVERE, null, ex);
//                            }
//                        }
//                        //D:*/
//
                        return false;
                    }else{
                    	dif += count;
                    }
                } else {
//        //D:/*
//                    if (gNode == 84748654765648L) {
//                        try {
//                            error("Missing information for label %s, queryNode %s at distance %d", FreebaseConstants.getPropertyMid(label), FreebaseConstants.convertLongToMid(qNode), i);
//                        } catch (IOException ex) {
//                            Logger.getLogger(PruningAlgorithm.class.getName()).log(Level.SEVERE, null, ex);
//                        }
//                    }
//                        //D:*/
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

    


}
