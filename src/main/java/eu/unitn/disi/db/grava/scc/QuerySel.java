/**
 * 
 */
package eu.unitn.disi.db.grava.scc;

import java.util.HashSet;
import java.util.Set;

import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;

/**
 * @author Zhaoyang
 *
 */
public class QuerySel {
	private Multigraph graph;
	private Multigraph query;
	private Long startingNode;
	public QuerySel(Multigraph graph, Multigraph query, Long startingNode) {
		this.graph = graph;
		this.query = query;
		this.startingNode = startingNode;
	}
	
	public double computeSelAdjNotCorrelated(double baseSel, Long crt, Set<Long> visited) {
		Set<Edge> adjEdges = new HashSet<>();
		visited.add(crt);
		for (Edge e : query.outgoingEdgesOf(crt)) {
			Long nextNode = e.getDestination().equals(crt) ? e.getSource() : e.getDestination();
			if (!visited.contains(nextNode)) {
				adjEdges.add(e);
			}
		}
		for (Edge e : query.incomingEdgesOf(crt)) {
			Long nextNode = e.getDestination().equals(crt) ? e.getSource() : e.getDestination();
			if (!visited.contains(nextNode)) {
				adjEdges.add(e);
			}
		}
		if (adjEdges.size() == 0) {
			return baseSel;
		}
		double min = baseSel;
		for (Edge e: adjEdges) {
			double labelSel = ((BigMultigraph)graph).getLabelFreq().get(e.getLabel()).getFrequency() / (double)graph.edgeSet().size();
			double nextSel = baseSel * labelSel;
			
			if (e.getDestination().equals(e.getSource())) {
				min = Math.min(min, nextSel);
				continue;
			}
			Long nextNode = e.getDestination().equals(crt) ? e.getSource() : e.getDestination();
//			System.out.println(crt + " " + nextNode + " " + nextSel + " " + labelSel);
			min = Math.min(min, computeSelAdjNotCorrelated(nextSel, nextNode, visited));
		}
		return min;
	}

}
