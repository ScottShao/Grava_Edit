/*
 * Copyright (C) 2012 Matteo Lissandrini <ml at disi.unitn.eu>
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
package eu.unitn.disi.db.exemplar.core;

import eu.unitn.disi.db.command.util.LoggableObject;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.MappedNode;
import eu.unitn.disi.db.grava.graphs.Multigraph;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * This is a gneric related query object
 * it maintains info of mapping from user query to the matched related query
 *
 * @author Matteo Lissandrini <ml at disi.unitn.eu>
 */

public abstract class RelatedQuery extends LoggableObject {
    /**
     * The query to map
     */
//    protected Multigraph query;

    /**
     * Weights
     */
//    protected Map<Long, Double> nodeWeights;
//    protected double totalWeight;

    RelatedQuery() {  } //A default constructor used for serialization

    /**
     * Constructor for the class
     *
     * @param query the query that needs to be mapped
     */
//    public RelatedQuery(Multigraph query) {
//        this.query = query;
////        this.nodeWeights = new HashMap<>();
////        this.totalWeight = 0;
//
//    }

//    public Multigraph getQuery() {
//        return query;
//    }




    /**
     *
     * @return a clone of the current RelatedQuery and mapping
     */
    public abstract RelatedQuery getClone();


    /**
     * Maps the passed query node to the given node in the graph
     *
     * @param queryNode
     * @param graphNode
     */
    public abstract void map(Long queryNode, Long graphNode);

    /**
     * Maps the passed query edge to the given edge in the graph
     *
     * @param queryEdge
     * @param graphEdge
     */
    public abstract  void map(Edge queryEdge, Edge graphEdge);


    /**
     * check whether the queryNode has already been mapped to a Graph Node
     *
     * @param queryNode
     * @return true if the queryNode has already been mapped to the Graph
     */
    public abstract  boolean hasMapped(Long queryNode);


    /**
     *
     * @return the nodes in the graph that are mapped to something
     */
    public abstract Set<MappedNode> getUsedNodes();



    /**
     * check whether the graphNode has already been mapped to a queryNpde
     *
     * @param graphNode
     * @return true if the graphNode has already been mapped to a queryNode
     */
    public abstract boolean isUsing(Long graphNode);



    /**
     *
     * @param queryNode
     * @return the list of graph nodes mapped to this query node
     */
    public abstract List<MappedNode> mapOf(Long queryNode);

    /**
     *
     * @param queryEdge
     * @return  the list of graph edges mapped to this query edge
     */
    public abstract List<Edge> mapOf(Edge queryEdge);


    /**
     * check whether the queryEdge has already been mapped to a graphEdge
     *
     * @param queryEdge
     * @return true if the queryEdge has already been mapped to the graph
     */
    public abstract boolean hasMapped(Edge queryEdge);

    /**
     * check whether the graphEdge has already been mapped to a queryEdge
     *
     * @param graphEdge
     * @return true if the graphEdge has already been mapped to a queryEdge
     */
    public abstract boolean isUsing(Edge graphEdge);


    /**
     *
     * @return the copy of the set of used edges
     */
    public abstract Set<String> getUsedEdgesIDs();


    /**
     *
     * @return the copy of the set of used edges
     */
    public abstract Set<Edge> getUsedEdges();



    /**
     * Builds a graph from the mapped nodes and edges
     *
     * @return
     */
    public abstract Multigraph buildRelatedQueryGraph();




    /**
     * Adds the weight of the node, replacing if an old one exists
     * @param c
     * @param w
     * @return the current total weight
     */
//    public double addWeight(Long c, Double w) {
//        Double old;
//        old = nodeWeights.put(c, w);
//        if (old != null) {
//            totalWeight -= old;
//            totalWeight += w;
//        } else {
//            totalWeight += w;
//        }
//
//        return totalWeight;
//    }

    /**
     *
     * @return the current total weight
     */
//    public double getTotalWeight() {
//        return totalWeight;
//    }

//    /**
//     * Compares based on the total weight
//     * @param obj
//     * @return
//     */
//    @Override
//    public int compareTo(Object obj) {
//
//        if (obj == null) {
//            throw new NullPointerException("Null object cannot be compared");
//        }
//
//        if (getClass() != obj.getClass()) {
//            throw new ClassCastException("Only RelatedQuery objects can be compared");
//        }
//
//        RelatedQuery other = (RelatedQuery) obj;
//        return (new Double(this.totalWeight)).compareTo(other.getTotalWeight());
//    }


    /**
     *
     * @param obj
     * @return true if the two RelatedQueries represent the same mapping.
     */
    @Override
    public abstract boolean equals(Object obj);

    /**
     *
     * @return string representation
     */
    
//    public List<Edge> getEdgeSet(){
//    	PriorityQueue<Edge> edgeQueue = new PriorityQueue<Edge>(10, new Comparator<Edge>() {
//            public int compare(Edge e1, Edge e2) {
//            	if (e1.getLabel().equals(e2.getLabel())) {
//            		return e1.getDestination() > e2.getDestination()? 1 : -1;
//            	}
//            	return e1.getLabel() > e2.getLabel()? 1 : -1;
//            }
//        });
//    	edgeQueue.addAll(query.edgeSet());
//    	List<Edge> sortedEdgeSet = new ArrayList<>();
//    	while(!edgeQueue.isEmpty()) {
//    		sortedEdgeSet.add(edgeQueue.poll());
//    	}
//    	return sortedEdgeSet;
//    }
//    @Override
//    public String toString(){
//
//    	String result = "";
//    	List<Edge> sortedEdge = this.getEdgeSet();
//    	for (Edge e : sortedEdge) {
//    		result += e + "\n";
//    	}
//    	return result;
//    }

    /**
     *
     * @return the hash code of this query based on the mapped edges
     */
    @Override
    public abstract int hashCode();


    /**
     * Adds an edge to a multigraph adding the nodes first when needed;
     * @param graph the graph to add to
     * @param edge the edd to add
     * @param stopIfMissing intterrupts
     * @return the new graph with added edge
     */
    public static Multigraph smartAddEdge(Multigraph graph, Edge edge, boolean stopIfMissing) {

        if (!graph.containsVertex(edge.getSource())) {
            if(stopIfMissing){
                throw new IllegalStateException("Missing source node for edge "+ edge);
            }
            graph.addVertex(edge.getSource());
        }
        if (!graph.containsVertex(edge.getDestination())) {
            if(stopIfMissing){
                throw new IllegalStateException("Missing destination node for edge "+ edge);
            }
            graph.addVertex(edge.getDestination());
        }
        //That a good mapping edge, add to the related query
        graph.addEdge(edge.getSource(), edge.getDestination(), edge.getLabel());
        return graph;
    }

}
