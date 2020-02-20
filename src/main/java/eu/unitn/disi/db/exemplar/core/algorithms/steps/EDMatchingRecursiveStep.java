/*
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
package eu.unitn.disi.db.exemplar.core.algorithms.steps;

import eu.unitn.disi.db.exemplar.core.EditDistanceQuery;
import eu.unitn.disi.db.exemplar.core.IsomorphicQuery;
import eu.unitn.disi.db.exemplar.core.RelatedQuery;
import eu.unitn.disi.db.exemplar.core.algorithms.EditDistanceQuerySearch;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.MappedNode;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.utils.Utilities;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public class EDMatchingRecursiveStep extends AlgorithmStep<EditDistanceQuery> {

    private final Long queryConcept;
    private final int threshold;
    private Map<Long, Set<MappedNode>> queryToGraph;
    private Map<Long, Set<Long>> queryToRawGraphNodes;
    private int cmpCount;
    private boolean isQuit;
    private int chunkSize;
    public EDMatchingRecursiveStep(int threadNumber, Iterator<MappedNode> kbConcepts, Long queryConcept, Multigraph query, Multigraph targetSubgraph, boolean limitComputation, boolean skipSave, int threshold, Map<Long, Set<MappedNode>> queryToGraph, int chunkSize) {
        super(threadNumber,kbConcepts,query, targetSubgraph, limitComputation, skipSave);
        this.queryConcept = queryConcept;
        this.threshold = threshold;
        this.queryToGraph = queryToGraph;
        this.queryToRawGraphNodes = queryToGraph.entrySet()
                .stream()
                .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().stream().map(MappedNode::getNodeID).collect(
                        Collectors.toSet())));
        this.cmpCount = 0;
        this.chunkSize = chunkSize;
    }

    public List<EditDistanceQuery> call() throws Exception {
        EditDistanceQuery relatedQuery;
        List<EditDistanceQuery> relatedQueriesPartial = new LinkedList<>();
        Set<EditDistanceQuery> relatedQueries = new HashSet<>();

        boolean warned = false;
        watch.start();
        int i = 0;
        this.isQuit = false;
        while (graphNodes.hasNext()) {
        	MappedNode node = graphNodes.next();
            try {
                relatedQuery = new EditDistanceQuery(query);
                //Map the first node
                relatedQuery.map(queryConcept, node);
                relatedQueriesPartial = createQueries(query, queryConcept, node, relatedQuery, 0);
                i++;
                if (this.isQuit) {
                	break;
                }
                if (relatedQueriesPartial != null) {
                    EditDistanceQuerySearch.answerCount += relatedQueriesPartial.size();
                    System.out.println("node:" + node + " size:" + relatedQueriesPartial.size());
                    relatedQueries.addAll(relatedQueriesPartial);
//                    for (EditDistanceQuery partial : relatedQueriesPartial) {
//                    	if(partial.getQuery().edgeSet().size() == query.edgeSet().size())
//                    		relatedQueries.add(partial);
//                    }
                    if (watch.getElapsedTimeMillis() > WARN_TIME) {
                        warn("More than " + MAX_RELATED + " partial isomorphic results");
                        warned = true;
                        if (limitComputation) {
                            warn("Computation interrupted after " + EditDistanceQuerySearch.answerCount + " partial isomorphic results");
                            break;
                        }
                        relatedQueriesPartial.clear();
                        EditDistanceQuerySearch.isBad = true;
                    }
                }
            } catch (OutOfMemoryError E) {
                if (relatedQueriesPartial != null) {
                    relatedQueriesPartial.clear();
                }
                EditDistanceQuerySearch.isBad = true;
                error("Memory exausted, so we are returning something but not everything.");
                System.gc();
                return new LinkedList<>(relatedQueries);
            }

            //if (watch.getElapsedTimeMillis() > WARN_TIME) {
            //    info("Computation %d [%d] took %d ms", threadNumber, Thread.currentThread().getId(), watch.getElapsedTimeMillis());
            //}
        }
//        System.out.println(this.cmpCount);
        watch.stop();
//        System.out.println(this + "future finished");
//        System.out.println(relatedQueries.size());
        System.out.println( "Answer size:" + relatedQueries.size());
        return new LinkedList<>(relatedQueries);
    }

    /**
     * Given a query, a starting node from the query, and a node from the
     * knowledgeBase , tries to build up a related query
     *
     * @param query
     * @param queryNode
     * @param graphNode
     * @return
     */
    public List<EditDistanceQuery> createQueries(Multigraph query, Long queryNode, MappedNode graphNode, EditDistanceQuery relatedQuery, int depth) {
        // Initialize the queries set
        //Given the current situation we expect to build more than one possible related query
//    	System.out.println("expanding:" + queryNode + " with " + graphNode);
    	List<EditDistanceQuery> relatedQueries = new ArrayList<>();
        relatedQueries.add(relatedQuery);

        // The graphEdges exiting from the query node passed
        Collection<Edge> queryEdgesOut = query.outgoingEdgesOf(queryNode);
        // The graphEdges entering the query node passed
        Collection<Edge> queryEdgesIn = query.incomingEdgesOf(queryNode);

        // The graphEdges in the KB exiting from the mapped node passed
        Collection<Edge> graphEdgesOut = graph.outgoingEdgesOf(graphNode.getNodeID());
        // The graphEdges in the KB entering the mapped node passed
        Collection<Edge> graphEdgesIn = graph.incomingEdgesOf(graphNode.getNodeID());

        // Null handling
        queryEdgesIn = queryEdgesIn == null ? new HashSet<Edge>() : queryEdgesIn;
        queryEdgesOut = queryEdgesOut == null ? new HashSet<Edge>() : queryEdgesOut;
        graphEdgesIn = graphEdgesIn == null ? new HashSet<Edge>() : graphEdgesIn;
        graphEdgesOut = graphEdgesOut == null ? new HashSet<Edge>() : graphEdgesOut;

        //debug("TEst %d map to  %d", queryNode, graphNode);

        //Optimization: if the queryEdges are more than the kbEdges, we are done, not isomorphic!
        if (queryEdgesIn.size() - graphEdgesIn.size() + queryEdgesOut.size() -  graphEdgesOut.size()
                + relatedQuery.getEdit() > threshold) {
            return null;
        }

        //All non mapped graphEdges from the query are put in one set
        Set<Edge> queryEdges = new HashSet<>();

        for (Edge edgeOut : queryEdgesOut) {
            if (!relatedQuery.hasMapped(edgeOut)) {
                queryEdges.add(edgeOut);
            }
        }

        for (Edge edgeIn : queryEdgesIn) {
            if (!relatedQuery.hasMapped(edgeIn)) {
                queryEdges.add(edgeIn);
            }
        }

        queryEdgesIn = null;
        queryEdgesOut = null;

        List<Edge> sortedEdges = sortEdge(queryEdges, query, queryNode);
        int currentSize = 0;

        if (queryNode.equals(graphNode.getNodeID())) {
            System.out.print("");
        }
        //Look if we can map all the outgoing/ingoing graphEdges of the query node
        for (Edge queryEdge : sortedEdges) {
//            if (queryNode.equals(5080955348L) && queryEdge.toString().equals("5080955348-[1000008074]->817683565648")) {
//                System.out.println("");
//            }
//        	System.out.println(" 1. Answer size increases " + (relatedQueries.size() - currentSize));

        	currentSize = relatedQueries.size();
            if (relatedQueries.size() > MAX_RELATED) {
                System.out.println(String.format("More than %d partial results", MAX_RELATED));
                isQuit = true;
                return relatedQueries;
            }
            //info("Trying to map the edge " + queryEdge);
            List<EditDistanceQuery> newRelatedQueries = new ArrayList<>();
            LinkedList<EditDistanceQuery> toTestRelatedQueries = new LinkedList<>();

             for (EditDistanceQuery current : relatedQueries) {
                if (current.hasMapped(queryEdge)) {
                    newRelatedQueries.add(current);
                } else {
                    toTestRelatedQueries.add(current);
                }
            }
            EditDistanceQuerySearch.interNum = Math.max(EditDistanceQuerySearch.interNum, toTestRelatedQueries.size());
            // reset, we do not want too many duplicates
            relatedQueries = new LinkedList<>();

            // If all candidated have this QueryEdge mapped, go to next
            if(toTestRelatedQueries.isEmpty()){
                relatedQueries = newRelatedQueries;
                continue;
            }

            // The label we are looking for
            Long label = queryEdge.getLabel();

            //is it isIncoming or outgoing ?
            boolean isIncoming = queryEdge.getDestination().equals(queryNode);

            List<Edge> graphEdges;
            // Look for graphEdges with the same label and same direction as the one from the query
            if(relatedQuery.getEdit() <= this.threshold){
            	graphEdges = new ArrayList<Edge>(graphEdgesIn);
            	graphEdges.addAll(graphEdgesOut);
            } else {
            	return null;
            }

            //loggable.debug("Matching with %d graphEdges", graphEdges.size() );
            // Do we found any?
            if (graphEdges.isEmpty()) {
                // If we cannot map graphEdges, this path is wrong
                return null;
            } else {
                //Cycle through all the possible graphEdges options,
                //they would be possibly different related queries
                int newCurrentSize = newRelatedQueries.size();
//                System.out.println("2 depth:" + depth  + "Test for related queries:" + toTestRelatedQueries.size());
                for (Edge graphEdge : graphEdges) {
                    if (queryEdge.toString().equals("5080955348-[1000008074]->817683565648") && graphEdge.toString().equals("8048756648-[1000008074]->525383745048")) {
                        System.out.println("");
                    }
//                    System.out.println("edit: " +  graphNode.getDist() +" query node:" + queryNode +  " graph node:" + graphNode.getNodeID() + " query edge:" + queryEdge + " graph edge:" + graphEdge);
//                	System.out.println(" 2.1 depth:" + depth +  " Query edge:" + queryEdge + " Test graph edge:" + graphEdge);
                    //Cycle through all the possible related queries retrieved up to now
                    //A new related query is good if it finds a match
                    Long queryNextNode;
                    MappedNode graphNextNode;
                    boolean isLabelDif = !graphEdge.getLabel().equals(queryEdge.getLabel());
                    int dis = isLabelDif ? 1:0;
                    if (isIncoming) {
                        queryNextNode = queryEdge.getSource();
                    } else {
                        queryNextNode = queryEdge.getDestination();
                    }
                    boolean graphEdgeIsIncoming = graphEdge.getDestination().equals(graphNode.getNodeID());
                    Long graphNextNodeLong = graphEdge.getSource().equals(graphNode.getNodeID()) ? graphEdge.getDestination() : graphEdge.getSource();
                    if (isIncoming ^ graphEdgeIsIncoming) {
                        dis = 1;
                    }

                    graphNextNode =  new MappedNode(graphNextNodeLong, graphEdge, relatedQuery.getEdit() + dis, graphEdgeIsIncoming, isLabelDif);


                    for (EditDistanceQuery tempRelatedQuery : toTestRelatedQueries) {

                    	if (newRelatedQueries.size()  > MAX_RELATED) {
                    		System.out.println(String.format("More than %d partial results", MAX_RELATED));
                    		this.isQuit = true;
                    		return relatedQueries.size() > 0 ? relatedQueries : null;
                    	}
                        if (watch.getElapsedTimeMillis() > QUIT_TIME) {
                            System.out.println(String.format("Time limit exceeded %d", QUIT_TIME));
                            this.isQuit = true;
                            return relatedQueries.size() > 0 ? relatedQueries : null;
                        }


                        if (tempRelatedQuery.isUsing(graphEdge) || tempRelatedQuery.isMappedAsDifferentNode(queryNextNode, graphNextNode)) {
                            //Ok this option is already using this edge,
                            //not a good choice go away
                            //it means that this query didn't found his match in this edge
                            continue;
                        }
                        Utilities.searchCount ++;
                        //Otherwise this edge can be mapped to the query edge if all goes well
                        EditDistanceQuery newRelatedQuery = tempRelatedQuery.getClone();

                        //check nodes similarity
                        //double nodeSimilarity = 0;
                        //if (isIncoming) {
                        //    nodeSimilarity = RelatedQuerySearch.conceptSimilarity(queryEdge.getSource(), graphEdge.getSource());
                        //} else {
                        //    nodeSimilarity = RelatedQuerySearch.conceptSimilarity(queryEdge.getDestination(), graphEdge.getDestination());
                        //}
                        //If the found edge peudo-destination is similar to the query edge pseudo-destination
                        //if (nodeSimilarity > RelatedQuerySearch.MIN_SIMILARITY) {
                        //The destination if outgoing the source if isIncoming


                        //Is this node coeherent with the structure?
                        if (edgeMatch(queryEdge, graphEdge, graphNode, graphNextNode, newRelatedQuery, this.threshold)) {
                            //That's a good edge!! Add it to this related query
                            newRelatedQuery.map(queryEdge, graphEdge);

                            //Map also the node
//                            System.out.println("next" + queryNextNode + " " + graphNextNode);
                            newRelatedQuery.map(queryNextNode, graphNextNode);
                            
                            if(isLabelDif){
                            	newRelatedQuery.incrementEdit();
                            }
                            //The query node that we are going to map
                            //Does it have graphEdges that we don't have mapped?
                            boolean needExpansion = false;
                            Collection<Edge> pseudoOutgoingEdges = query.incomingEdgesOf(queryNextNode);
                            Long queryPrevNode = null;
                            if (pseudoOutgoingEdges.size() > 0) {
                                for (Edge pseudoEdge : pseudoOutgoingEdges) {
                                    needExpansion = !newRelatedQuery.hasMapped(pseudoEdge) && !pseudoEdge.equals(queryEdge);
                                    queryPrevNode = pseudoEdge.getDestination().equals(queryNextNode)?pseudoEdge.getSource():pseudoEdge.getDestination();
                                    needExpansion = needExpansion && !queryPrevNode.equals(queryNode);
                                    if (needExpansion) {
                                        break;
                                    }
                                }
                            }

                            pseudoOutgoingEdges = query.outgoingEdgesOf(queryNextNode);
                            if (!needExpansion && pseudoOutgoingEdges.size() > 0) {
                                for (Edge pseudoEdge : pseudoOutgoingEdges) {
                                    needExpansion = !newRelatedQuery.hasMapped(pseudoEdge) && !pseudoEdge.equals(queryEdge);
                                    queryPrevNode = pseudoEdge.getDestination().equals(queryNextNode)?pseudoEdge.getSource():pseudoEdge.getDestination();
                                    needExpansion = needExpansion && !queryPrevNode.equals(queryNode);
                                    if (needExpansion) {
                                        break;
                                    }
                                }
                            }

                            //Lookout! We need to check the outgoing part, if we did not already
                            if (needExpansion) {
                                // Possible outgoing branches
                                List<EditDistanceQuery> tmpRelatedQueries;
                                //Go find them!
                                //log("Go find mapping for: " + queryNextNode + " // " + graphNextNode);
                                tmpRelatedQueries = createQueries(query, queryNextNode, graphNextNode, newRelatedQuery, depth + 1);


//                                System.out.println("    3. depth:" + depth  + " query next node:" + queryNextNode + " graph next node:" + graphNextNode
//                                        + " query edge:" + queryEdge + " graph edge:" + graphEdge +
//                                        " related size:" + (tmpRelatedQueries == null? 0 :tmpRelatedQueries.size()));

                                //Did we find any?
                                if (tmpRelatedQueries != null) {
                                    //Ok so we found some, they are all good to me
                                    //More possible related queries
                                    //They already contain the root
//                                    System.out.println("     3.1 current size:" + newRelatedQueries.size() + " increases size:" + tmpRelatedQueries.size());
                                    for (EditDistanceQuery branch : tmpRelatedQueries) {
                                        //All these related queries have found in this edge their match
                                        newRelatedQueries.add(branch);
                                    }

                                }
                                // else {
                                // This query didn't find in this edge its match
                                // continue;
                                //}
                            } else {
                                //log("Complete query " + relatedQuery);
                                //this related query has found in this edge is map
                                //newRelatedQuery.map(queryNextNode, graphNextNode);
                                newRelatedQueries.add(newRelatedQuery);
                            }
                        }
                    }
//                    System.out.println(" 2.2 depth:" + depth + " increase size to:" +newRelatedQueries.size()  + " increased: " + (newRelatedQueries.size() - newCurrentSize) + " with graph edge " + graphEdge);
                    newCurrentSize = newRelatedQueries.size();
                }
            }
            //after this cycle we should have found some, how do we check?
            if (newRelatedQueries.isEmpty()) {
                return null;
            } else {
                //basically in the *new* list are the related queries still valid and growing
                relatedQueries = newRelatedQueries;
            }

        }
        return relatedQueries.size() > 0 ? relatedQueries : null;
    }
    
    public static List<Edge> sortEdge(final Set<Edge> edges, final Multigraph query, Long queryNode) {
		List<Edge> sortedEdges = new ArrayList<>();
    	PriorityQueue<Edge> pq = new PriorityQueue<>( new Comparator<Edge>(){
    		public int compare(Edge e1, Edge e2) {
    			if (e1.getLabel().equals(0L)) {
    				return 1;
    			} else if (e2.getLabel().equals(0L)) {
    				return -1;
    			} else {
                    boolean isIncoming1 = e1.getDestination().equals(queryNode);
                    boolean isIncoming2 = e2.getDestination().equals(queryNode);
                    long queryNextNode1;
                    long queryNextNode2;
                    if (isIncoming1) {
                        queryNextNode1 = e1.getSource();
                    } else {
                        queryNextNode1 = e1.getDestination();
                    }
                    if (isIncoming2) {
                        queryNextNode2 = e2.getSource();
                    } else {
                        queryNextNode2 = e2.getDestination();
                    }
    				return query.outDegreeOf(queryNextNode2) + query.inDegreeOf(queryNextNode2) -
                            query.outDegreeOf(queryNextNode1) - query.inDegreeOf(queryNextNode1);
    			}
    		}
    	});
    	for (Edge qe : edges) {
    		pq.add(qe);
    	}
    	while(!pq.isEmpty()) {
    		sortedEdges.add(pq.poll());
    	}
    	return sortedEdges;
	}
    protected boolean edgeMatch(Edge queryEdge, Edge graphEdge, MappedNode graphNode, MappedNode graphNextNode, EditDistanceQuery r, int threshold) {
    	
        if (queryEdge.getLabel() != graphEdge.getLabel().longValue() && r.getEdit() >= threshold) {
            return false;
        }

        Long querySource = null;
        Long queryDestination = null;
        MappedNode graphSource = null;
        MappedNode graphDestination = null;


        if (r != null) {
            if (r.isUsing(graphEdge)) {
                return false;
            }

            querySource = queryEdge.getSource();
            graphSource = graphEdge.getSource().equals(graphNode.getNodeID())? graphNode:graphNextNode;//TODO bug for self cycle

            boolean mappedSource = r.hasMapped(querySource);
            boolean usingSource = r.isUsing(graphSource);

            if (usingSource && !mappedSource) {
                return false;
            }

            queryDestination = queryEdge.getDestination();
            graphDestination = graphEdge.getDestination().equals(graphNode.getNodeID())? graphNode:graphNextNode;

            boolean mappedDestination = r.hasMapped(queryDestination);
            boolean usingDestination = r.isUsing(graphNextNode);
            if (usingDestination && !mappedDestination) {
                return false;
            }
                        
            if (mappedSource && graphSource.getNodeID() != r.isomorphicMapOf(querySource).getNodeID()) {
//            	System.out.println(graphSource + " " + querySource + " " + r.isomorphicMapOf(querySource) + " next " + graphNextNode);
                return false;
            }

            if (mappedDestination && graphDestination.getNodeID() != r.isomorphicMapOf(queryDestination).getNodeID()) {
            	
                return false;
            }

            if (usingSource && !r.mappedAs(graphNode).equals(querySource)) {
                return false;
            }

            if (usingDestination && !r.mappedAs(graphNextNode).equals(queryDestination)) {
                return false;
            }

        }
        return true;
    }
    /**
     *
     * @param queryEdge
     * @param graphEdge
     * @param r
     * @return
     */
    protected boolean edgeMatch(Edge queryEdge, Edge graphEdge, IsomorphicQuery r) {

        if (queryEdge.getLabel() != graphEdge.getLabel().longValue()) {
            return false;
        }

        Long querySource = null;
        Long queryDestination = null;
        Long graphSource = null;
        Long graphDestination = null;

        if (r != null) {
            if (r.isUsing(graphEdge)) {
                return false;
            }

            querySource = queryEdge.getSource();
            graphSource = graphEdge.getSource();

            boolean mappedSource = r.hasMapped(querySource);
            boolean usingSource = r.isUsing(graphSource);

            if (usingSource && !mappedSource) {
                return false;
            }

            queryDestination = queryEdge.getDestination();
            graphDestination = graphEdge.getDestination();

            boolean mappedDestination = r.hasMapped(queryDestination);
            boolean usingDestination = r.isUsing(graphDestination);
            if (usingDestination && !mappedDestination) {
                return false;
            }

            if (mappedSource && !r.isomorphicMapOf(querySource).equals(graphSource)) {
                return false;
            }

            if (mappedDestination && !r.isomorphicMapOf(queryDestination).equals(graphDestination)) {
                return false;
            }

            if (usingSource && !r.mappedAs(graphSource).equals(querySource)) {
                return false;
            }

            if (usingDestination && !r.mappedAs(graphDestination).equals(queryDestination)) {
                return false;
            }

        }
        return true;
    }

    /**
     * Checks if, for a given node, it exist an <b>outgoing</b>
     * with that label and returns all the graphEdges found
     *
     * @param label the label we are looking for
     * @param graphEdges the knoweldgebase graphEdges
     * @return labeled graphEdges, can be empty
     */
    public static List<Edge> findEdges(Long label, Collection<Edge> graphEdges) {

        // Compare to the graphEdges in the KB exiting from the mapped node passed
        List<Edge> edges = new ArrayList<>();

        for (Edge Edge : graphEdges) {
            if (label == Edge.GENERIC_EDGE_LABEL || Edge.getLabel().longValue() == label) {
                edges.add(Edge);
            }
        }
        return edges;
    }

	public int getCmpCount() {
		return cmpCount;
	}

	public void setCmpCount(int cmpCount) {
		this.cmpCount = cmpCount;
	}
    
//    public static List<Edge> findEdgeWithEdit(Long label, Collection<Edge> graphEdges, int threshold){
//    	// Compare to the graphEdges in the KB exiting from the mapped node passed
//        List<Edge> edges = new ArrayList<>();
//
//        for (Edge Edge : graphEdges) {
//            if (label == Edge.GENERIC_EDGE_LABEL || Edge.getLabel().longValue() == label) {
//                edges.add(Edge);
//            }
//        }
//        return edges;
//    }

}
