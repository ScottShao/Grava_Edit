/**
 * 
 */
package eu.unitn.disi.db.grava.scc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;

/**
 * @author Zhaoyang
 *
 */
public class Cost {
	public static double cost = 0;
	/**
	 * 
	 */
	public Cost() {
		// TODO Auto-generated constructor stub
	}
	
	public static int getCandidatesNum(Multigraph query, Long crtNode, Multigraph graph) {
		Set<Edge> queryEdges = new HashSet<>();
		for (Edge ie : query.incomingEdgesOf(crtNode)) {
				queryEdges.add(ie);
		}
		
		for (Edge oe : query.outgoingEdgesOf(crtNode)) {
				queryEdges.add(oe);
		}
		
		Edge minEdge = sortEdge(queryEdges, graph);
		if (minEdge.getLabel().equals(0L))
			return graph.vertexSet().size();
//		System.out.println(minEdge);
		return ((BigMultigraph)graph).getLabelFreq().get(minEdge.getLabel()).getFrequency();
	}
	
	public static int getWCCandidatesNum(Multigraph query, Long crtNode, Multigraph graph) {
		Set<Edge> queryEdges = new HashSet<>();
		for (Edge ie : query.incomingEdgesOf(crtNode)) {
				queryEdges.add(ie);
		}
		
		for (Edge oe : query.outgoingEdgesOf(crtNode)) {
				queryEdges.add(oe);
		}
		
		Edge minEdge = sortEdge(queryEdges, graph);
		if (minEdge.getLabel().equals(0L))
			return graph.vertexSet().size();
//		System.out.println(minEdge);
		return ((BigMultigraph)graph).getLabelFreq().get(minEdge.getLabel()).getFrequency();
	}
	
	public static Edge sortEdge(Set<Edge> edges, Multigraph graph) {
		List<Edge> sortedEdges = new ArrayList<>();
    	PriorityQueue<Edge> pq = new PriorityQueue<>( new Comparator<Edge>(){
    		public int compare(Edge e1, Edge e2) {
    			if (e1.getLabel().equals(0L)) {
    				return 1;
    			} else if (e2.getLabel().equals(0L)) {
    				return -1;
    			} else {
    				return (int)(((BigMultigraph)graph).getLabelFreq().get(e1.getLabel()).getFrequency() - ((BigMultigraph)graph).getLabelFreq().get(e2.getLabel()).getFrequency());
    			}
    		}
    	});
    	for (Edge qe : edges) {
    		pq.add(qe);
    	}

    	return pq.peek();
	}
	
	public static List<Edge> getSortedEdges(List<Edge> edges, Multigraph graph) {
		List<Edge> sortedEdges = new ArrayList<>();
    	PriorityQueue<Edge> pq = new PriorityQueue<>(new Comparator<Edge>(){
    		public int compare(Edge e1, Edge e2) {
    			if (e1.getLabel().equals(0L)) {
    				return 1;
    			} else if (e2.getLabel().equals(0L)) {
    				return -1;
    			} else {
    				
    				return (int)(((BigMultigraph)graph).getLabelFreq().get(e1.getLabel()).getFrequency() - ((BigMultigraph)graph).getLabelFreq().get(e2.getLabel()).getFrequency());
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
	
	 public static List<Edge> getSortedEdges(Set<Edge> edges, Multigraph graph) {
			List<Edge> sortedEdges = new ArrayList<>();
	    	PriorityQueue<Edge> pq = new PriorityQueue<>(new Comparator<Edge>(){
	    		public int compare(Edge e1, Edge e2) {
	    			if (e1.getLabel().equals(0L)) {
	    				return 1;
	    			} else if (e2.getLabel().equals(0L)) {
	    				return -1;
	    			} else {
	    				
	    				return (int)(((BigMultigraph)graph).getLabelFreq().get(e1.getLabel()).getFrequency() - ((BigMultigraph)graph).getLabelFreq().get(e2.getLabel()).getFrequency());
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
	
	 public static void estimateMaxCostWithLabelMaxNum(Multigraph query, Long crtNode, int AVG_DEGREE, Multigraph graph, Set<Edge> visited, double multiplier) {
			Set<Edge> queryEdges = new HashSet<>();
			for (Edge ie : query.incomingEdgesOf(crtNode)) {
				if (!visited.contains(ie))
					queryEdges.add(ie);
			}
			
			for (Edge oe : query.outgoingEdgesOf(crtNode)) {
				if (!visited.contains(oe))
					queryEdges.add(oe);
			}
			List<Edge> sortedEdge = getSortedEdges(queryEdges, graph);
			HashMap<Long, Integer> labelMax = ((BigMultigraph)graph).getLabelMax();
//			System.out.println(labelMax.size());
			for (Edge qe : sortedEdge) {
				double costTemp;
				if (qe.getLabel().equals(0L)) {
					costTemp = AVG_DEGREE;
				} else {
					costTemp = labelMax.get(qe.getLabel());
				}
				multiplier *= costTemp;
				cost += multiplier;
				visited.add(qe);
				Long next = qe.getDestination().equals(crtNode) ? qe.getSource() : qe.getDestination();
				estimateMaxCostWithLabelMaxNum(query, next, AVG_DEGREE, graph, visited, multiplier);
			}
	}
	
	 public static void estimateExactCost(Multigraph query, Long crtNode, int AVG_DEGREE, Multigraph graph, Set<Edge> visited, double multiplier) {
			Set<Edge> queryEdges = new HashSet<>();
			for (Edge ie : query.incomingEdgesOf(crtNode)) {
				if (!visited.contains(ie))
					queryEdges.add(ie);
			}
			
			for (Edge oe : query.outgoingEdgesOf(crtNode)) {
				if (!visited.contains(oe))
					queryEdges.add(oe);
			}
			List<Edge> sortedEdge = getSortedEdges(queryEdges, graph);
			for (Edge qe : sortedEdge) {
				if (visited.contains(qe)) {
					continue;
				}
//				System.out.println(qe);
				double costTemp;
				if (qe.getLabel().equals(0L)) {
					double em;
					costTemp = AVG_DEGREE;
					for (Edge ee : visited) {
						if (ee.getLabel().equals(0L)) continue;//
						em = AVG_DEGREE * ((BigMultigraph)graph).getLabelFreq().get(ee.getLabel()).getFrequency() / (double)graph.edgeSet().size();
						costTemp -= em;
					}
					
				} else {
					costTemp = AVG_DEGREE * ((BigMultigraph)graph).getLabelFreq().get(qe.getLabel()).getFrequency() / (double)graph.edgeSet().size();
				}
				multiplier *=  costTemp;
				cost += multiplier;
				visited.add(qe);
				Long next = qe.getDestination().equals(crtNode) ? qe.getSource() : qe.getDestination();
				estimateExactCost(query, next, AVG_DEGREE, graph, visited, multiplier);
			}
	}
	 
//	 public static void estimateEdExactCost(Multigraph query, Long crtNode, int AVG_DEGREE, Multigraph graph, List<Edge> visited) {
//			List<Edge> queryEdges = new ArrayList<>();
//			for (Edge ie : query.incomingEdgesOf(crtNode)) {
//				if (!visited.contains(ie))
//					queryEdges.add(ie);
//			}
//			
//			for (Edge oe : query.outgoingEdgesOf(crtNode)) {
//				if (!visited.contains(oe))
//					queryEdges.add(oe);
//			}
//			List<Edge> sortedEdge = getSortedEdges(queryEdges, graph);
//			HashMap<Long, Integer> labelMax = ((BigMultigraph)graph).getLabelMax();
//			List<Long> nextNodes = new ArrayList<>();
//			for (Edge qe : sortedEdge) {
//				visited.add(qe);
//				double total = 0;
//				double temp = choose(AVG_DEGREE, visited.size());
//				for (int i = 0; i < visited.size(); i++) {
//					temp *= ((BigMultigraph)graph).getLabelFreq().get(visited.get(i).getLabel()).getFrequency() / (double)graph.edgeSet().size();
//				}
//				cost += Math.pow(AVG_DEGREE, visited.size()) * temp;
//				for (int i = 0; i < visited.size(); i++) {
//					double t = (((BigMultigraph)graph).getLabelFreq().get(visited.get(i).getLabel()).getFrequency() / (double)graph.edgeSet().size());
//					total += Math.pow(AVG_DEGREE, visited.size()) * (1 - t) * temp / t;
//				}
//				
//				cost += total;
//				
//				Long next = qe.getDestination().equals(crtNode) ? qe.getSource() : qe.getDestination();
//				estimateEdExactCost(query, next, AVG_DEGREE, graph, visited);
//			}
//	}
	 
	 public static void estimateEdExactCost(Multigraph query, Long crtNode, int AVG_DEGREE, Multigraph graph, List<Edge> visited) {
			List<Edge> queryEdges = new ArrayList<>();
			for (Edge ie : query.incomingEdgesOf(crtNode)) {
				if (!visited.contains(ie))
					queryEdges.add(ie);
			}
			
			for (Edge oe : query.outgoingEdgesOf(crtNode)) {
				if (!visited.contains(oe))
					queryEdges.add(oe);
			}
			List<Edge> sortedEdge = getSortedEdges(queryEdges, graph);
			for (Edge qe : sortedEdge) {
//				System.out.println(qe);
				double costTemp;
				if (qe.getLabel().equals(0L)) {
					costTemp = AVG_DEGREE;
				} else {
					costTemp = AVG_DEGREE * ((BigMultigraph)graph).getLabelFreq().get(qe.getLabel()).getFrequency() / (double)graph.edgeSet().size();
				}
				double crt = 0;
				for (int i = 0; i < visited.size(); i++) {
					double total = 1;
					for (int j = 0; j < visited.size(); j++) {
						double s = ((BigMultigraph)graph).getLabelFreq().get(visited.get(j).getLabel()).getFrequency() / (double)graph.edgeSet().size();
						if (i == j) {
							s = 1 - s;
						}
						total *= s * AVG_DEGREE;
					}
					crt += total;
				}
				cost += crt * costTemp;
				double total = 1;
				for (int i = 0; i < visited.size(); i++) {
					double s = ((BigMultigraph)graph).getLabelFreq().get(visited.get(i).getLabel()).getFrequency() / (double)graph.edgeSet().size();
					total *= s * AVG_DEGREE;
				}
				cost += total * AVG_DEGREE;
				visited.add(qe);
				Long next = qe.getDestination().equals(crtNode) ? qe.getSource() : qe.getDestination();
				estimateEdExactCost(query, next, AVG_DEGREE, graph, visited);
			}
	}
	 public static double choose(int x, int y) {
		    if (y < 0 || y > x) return 0;
		    if (y > x/2) {
		        // choose(n,k) == choose(n,n-k), 
		        // so this could save a little effort
		        y = x - y;
		    }

		    double denominator = 1.0, numerator = 1.0;
		    for (int i = 1; i <= y; i++) {
		        denominator *= i;
		        numerator *= (x + 1 - i);
		    }
		    return numerator / denominator;
		}
	public static void estimateMaxCost(Multigraph query, Long crtNode, Multigraph graph, int avgDegree, Set<Edge> visited, double multiplier) {
		Set<Edge> queryEdges = new HashSet<>();
		for (Edge ie : query.incomingEdgesOf(crtNode)) {
			if (!visited.contains(ie))
				queryEdges.add(ie);
		}
		
		for (Edge oe : query.outgoingEdgesOf(crtNode)) {
			if (!visited.contains(oe))
				queryEdges.add(oe);
		}
		
		List<Edge> sortedEdge = getSortedEdges(queryEdges, graph);
		
		for (Edge qe : sortedEdge) {
			double costTemp = avgDegree / (double) queryEdges.size();
			multiplier *= costTemp;
			cost += multiplier;
			visited.add(qe);
			Long next = qe.getDestination().equals(crtNode) ? qe.getSource() : qe.getDestination();
			estimateMaxCost(query, next, graph, avgDegree, visited, multiplier);
		}
	}
	
	public static double estimateQueryCost(Multigraph query, Long startingNode, Multigraph graph, int avgDegree) {
		double cost = graph.vertexSet().size();
		Set<Long> visited = new HashSet<Long>();
		Queue<Long> que = new LinkedList<>();
		que.offer(startingNode);
		
		while (!que.isEmpty()) {
			int size = que.size();
			int minFreq = 1;
			for (int i = 0; i < size; i++) {
				Long crtNode = que.poll();
				visited.add(crtNode);
//				System.out.println("cur:" + crtNode);
				Collection<Edge> edgeSet = query.incomingEdgesOf(crtNode);
				
				for (Edge e : edgeSet) {
					if (visited.contains(e.getSource())) continue;
					que.add(e.getSource());
					if (e.getLabel().equals(0L)) continue;
					int crtFreq = ((BigMultigraph)graph).getLabelFreq().get(e.getLabel()).getFrequency();
					if (crtFreq < minFreq) minFreq = crtFreq;
//					System.out.println("anot:" +e.getSource());
				}
				
				edgeSet = query.outgoingEdgesOf(crtNode);
				
				for (Edge e : edgeSet) {
					if (visited.contains(e.getDestination())) continue;
					que.add(e.getDestination());
					if (e.getLabel().equals(0L)) continue;
					int crtFreq = ((BigMultigraph)graph).getLabelFreq().get(e.getLabel()).getFrequency();
					if (crtFreq < minFreq) minFreq = crtFreq;
				}
			}
			cost *= minFreq/(double)graph.edgeSet().size() * avgDegree;
//			System.out.println(cost);
		}
		System.out.println("done");
		return cost;
	}
	
	
	
	public static double estimateEdCost(Multigraph query, Long startingNode, Multigraph graph, int avgDegree) {
		double cost = graph.vertexSet().size();
		Set<Long> visited = new HashSet<Long>();
		Queue<Long> que = new LinkedList<>();
		List<Integer> freq = new ArrayList<>();
		que.offer(startingNode);
		
		while (!que.isEmpty()) {
			int size = que.size();
			int minFreq = 1;
			for (int i = 0; i < size; i++) {
				Long crtNode = que.poll();
				visited.add(crtNode);
//				System.out.println("cur:" + crtNode);
				Collection<Edge> edgeSet = query.incomingEdgesOf(crtNode);
				
				for (Edge e : edgeSet) {
					if (visited.contains(e.getSource())) continue;
					que.add(e.getSource());
					if (e.getLabel().equals(0L)) continue;
					int crtFreq = ((BigMultigraph)graph).getLabelFreq().get(e.getLabel()).getFrequency();
					if (crtFreq < minFreq) minFreq = crtFreq;
//					System.out.println("anot:" +e.getSource());
				}
				
				edgeSet = query.outgoingEdgesOf(crtNode);
				
				for (Edge e : edgeSet) {
					if (visited.contains(e.getDestination())) continue;
					que.add(e.getDestination());
					if (e.getLabel().equals(0L)) continue;
					int crtFreq = ((BigMultigraph)graph).getLabelFreq().get(e.getLabel()).getFrequency();
					if (crtFreq < minFreq) minFreq = crtFreq;
				}
			}
			cost *= minFreq/(double)graph.edgeSet().size() * avgDegree;
			freq.add(minFreq);
//			System.out.println(cost);
		}
		double temp = cost;
		for (Integer ll : freq) {
			double crt = ll / (double)graph.edgeSet().size() * avgDegree;
			cost += temp/crt * (1 - crt);
		}
		return cost;
	}
	
	private static void bfs(Multigraph query, Long startingNode, Multigraph graph) {
		Set<Long> visited = new HashSet<Long>();
		Queue<Long> que = new LinkedList<>();
		que.offer(startingNode);
		
		while (!que.isEmpty()) {
			int size = que.size();
			int min = 1;
			for (int i = 0; i < size; i++) {
				Long crtNode = que.poll();
				visited.add(crtNode);
//				System.out.println("cur:" + crtNode);
				Collection<Edge> edgeSet = query.incomingEdgesOf(crtNode);
				
				for (Edge e : edgeSet) {
					que.add(e.getSource());
					if (e.getLabel().equals(0L)) continue;
					
//					System.out.println("anot:" +e.getSource());
				}
				
				edgeSet = query.outgoingEdgesOf(crtNode);
				
				for (Edge e : edgeSet) {
					que.add(e.getDestination());
				}
			}
		}
	}

}
