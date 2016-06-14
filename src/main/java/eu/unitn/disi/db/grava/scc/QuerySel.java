/**
 * 
 */
package eu.unitn.disi.db.grava.scc;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
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
	public static double seedSel = 1;
	class Event {
		boolean isPos;
		double sel;
		
		public Event(boolean isPos, double sel) {
			this.isPos = isPos;
			this.sel = sel;
		}
		
		public Event(double sel) {
			this.isPos = true;
			this.sel = sel;
		}
		
		public String toString() {
			return sel + " " + isPos;
		}
	}
	
	public void test() {
		Event e1 = new Event(true, 7.938973456214836E-4);
		Event e2 = new Event(true, 7.938973456214836E-4);
		Event e3 = new Event(true, 0.009388699043871457);
		List<Event> list = new ArrayList<>();
		list.add(e1);
		list.add(e2);
		list.add(e3);
		System.out.println("test:" + prob(list, 81));
	}
	public QuerySel(Multigraph graph, Multigraph query, Long startingNode) {
		this.graph = graph;
		this.query = query;
		this.startingNode = startingNode;
	}
	
	public double getCanNumber(Long crt, int degree, int dn) {
//		System.out.println("wc :" + prob(crt, degree, dn));
		return graph.vertexSet().size() * prob(crt, degree, dn);
	}
	
	public double getEdCanNumber(Long crt, int degree, int dn) {
		return graph.vertexSet().size() * edProb(crt, degree, dn);
	}
	
	public double edProb(Long crt, int degree, int dn) {
		Set<Long> visited = new HashSet<>();
		LinkedList<Long> queue = new LinkedList<>();
		List<List<Event>> neighbourhood = new ArrayList<>();
		queue.add(crt);
		int level = 1;
		while(!queue.isEmpty()) {
			if (level > dn) {
				break;
			}
			List<Event> n = new ArrayList<>();
			int len = queue.size();
			for (int i = 0; i < len; i++) {
				Long next = queue.poll();
				if (visited.contains(next)) {
					continue;
				}
				visited.add(next);
				for (Edge e : query.outgoingEdgesOf(next)) {
					Long nextNode = e.getDestination().equals(next) ? e.getSource() : e.getDestination();
					if (!visited.contains(nextNode) || nextNode.equals(next)) {
						double sel;
						queue.add(nextNode);
						if (e.getLabel().equals(0L)) {
							sel = 1;
						} else {
							sel = ((BigMultigraph)graph).getLabelFreq().get(e.getLabel()).getFrequency() / (double)graph.edgeSet().size();
							n.add(new Event(sel));
						}
						
					}
				}
				for (Edge e : query.incomingEdgesOf(next)) {
					Long nextNode = e.getDestination().equals(next) ? e.getSource() : e.getDestination();
					if (!visited.contains(nextNode) || nextNode.equals(next)) {
						double sel;
						queue.add(nextNode);
						if (e.getLabel().equals(0L)) {
							sel = 1;
						} else {
							sel = ((BigMultigraph)graph).getLabelFreq().get(e.getLabel()).getFrequency() / (double)graph.edgeSet().size();
							n.add(new Event(sel));
						}
						
					}
				}
			}
			neighbourhood.add(n);
			level++;
		}
		
		double p = 1;
		for (int i = 0; i < neighbourhood.size(); i++){
			p *= prob(neighbourhood.get(i), (int)Math.pow(degree, i + 1));
		}
		
		for (int i = 0; i < dn; i++) {
			List<Event> l = neighbourhood.get(i);
			for (int j = 0; j < l.size(); j++) {
				List<Event> newList = new ArrayList<>();
				double temp = 1;
				for (int k = 0; k < l.size(); k++) {
					if (k == j) {
						newList.add(new Event(!l.get(k).isPos, l.get(k).sel));
					} else {
						newList.add(new Event(l.get(k).isPos, l.get(k).sel));
					}
				}
				temp *= prob(newList, (int)Math.pow(degree, i + 1));
				temp *= prob(neighbourhood.get(i == 0 ? 1 : 0), (int)Math.pow(degree, i == 0 ? 2 : 1));
				p += temp;
			}
		}
		return p;
	}
	
	public double prob(Long crt, int degree, int dn) {
		Set<Long> visited = new HashSet<>();
		LinkedList<Long> queue = new LinkedList<>();
		List<List<Event>> neighbourhood = new ArrayList<>();
		visited.add(crt);
		queue.add(crt);
		int level = 1;
		while(!queue.isEmpty()) {
			if (level > dn) {
				break;
			}
			List<Event> n = new ArrayList<>();
			int len = queue.size();
			for (int i = 0; i < len; i++) {
				Long next = queue.poll();
				for (Edge e : query.outgoingEdgesOf(next)) {
					Long nextNode = e.getDestination().equals(next) ? e.getSource() : e.getDestination();
					if (!visited.contains(nextNode)) {
						double sel;
						queue.add(nextNode);
						if (e.getLabel().equals(0L)) {
							sel = 1;
						} else {
							sel = ((BigMultigraph)graph).getLabelFreq().get(e.getLabel()).getFrequency() / (double)graph.edgeSet().size();
							n.add(new Event(sel));
						}
						
					}
				}
				for (Edge e : query.incomingEdgesOf(next)) {
					Long nextNode = e.getDestination().equals(next) ? e.getSource() : e.getDestination();
					if (!visited.contains(nextNode)) {
						double sel;
						queue.add(nextNode);
						if (e.getLabel().equals(0L)) {
							sel = 1;
						} else {
							sel = ((BigMultigraph)graph).getLabelFreq().get(e.getLabel()).getFrequency() / (double)graph.edgeSet().size();
							n.add(new Event(sel));
						}
						
					}
				}
			}
			neighbourhood.add(n);
			level++;
		}
		double p = 1;
		for (int i = 0; i < neighbourhood.size(); i++){
			p *= prob(neighbourhood.get(i), (int)Math.pow(degree, i + 1));
		}
//		System.out.println(p);
		return p;
	}
	
	
	public double prob(List<Event> sels, int degree) {
		if (sels.size() == 0) {
			return 1;
		} else if (sels.size() == 1) {
			Event event = sels.get(0);
			if (event.isPos)
				return 1- Math.pow(1 - event.sel, degree);
			else
				return Math.pow(1 - event.sel, degree);
		} else {
			double p = 0;
			List<Event> a = new ArrayList<>();
			int i = 0;
			for (; i < sels.size(); i++) {
				Event en = sels.get(i);
				if (en.isPos) {
					break;
				} else {
					a.add(new Event(en.isPos, en.sel));
				}
			}
			
			for (int j = i + 1; j < sels.size(); j++) {
				Event en = sels.get(j);
				a.add(new Event(en.isPos, en.sel));
			}
			
			if (isAllNeg(a)) {
				p += computeP(a, degree);
			} else {
				p += prob(a, degree);
			}
			List<Event> b = new ArrayList<>();
			for (int j = 0; j < sels.size(); j++) {
				Event en = sels.get(j);
				if (j == i) {
					b.add(new Event(!en.isPos, en.sel));
				} else {
					b.add(new Event(en.isPos, en.sel));
				}
			}
			if (isAllNeg(b)) {
				p -= computeP(b, degree);
			} else {
				p -= prob(b, degree);
			}
			return p;
		}
	}
	
	private double computeP(List<Event> events, int degree) {
			double a = 1;
			for (Event en : events) {
				a -= en.sel;
			}
			return Math.pow(a, degree);
		
	}
	private boolean isAllNeg(List<Event> events) {
		for (Event e : events) {
			if (e.isPos) 
				return false;
		}
		return true;
	}
	
	public double computeSelAdjNotCorrelated(double baseSel, Long crt, Set<Long> visited, int depth, int max) {
		if (depth >= max) {
			return 1;
		}
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
			min = Math.min(min, computeSelAdjNotCorrelated(nextSel, nextNode, visited, depth + 1, max));
		}
		return min;
	}
	
	public double computeSelAllNotCorrelated(Multigraph query) {
		double sel = 1;
		for (Edge e : query.edgeSet()) {
			sel *= ((BigMultigraph)graph).getLabelFreq().get(e.getLabel()).getFrequency() / (double)graph.edgeSet().size();
		}
		return sel;
	}
	
	public double computeWCCandidates(double baseSel,  Long crt, int degree, int max) {
		Queue<Long> queue = new LinkedList<>();
		Set<Long> visited = new HashSet<>();
		double num = 0;
		visited.add(crt);
		queue.add(crt);
		int level = 0;
		while (!queue.isEmpty() && level < max) {
			double temp = 1;
			int size = queue.size();
			for (int i = 0; i < size; i++) {
				crt = queue.poll();
				for (Edge e : query.outgoingEdgesOf(crt)) {
					Long nextNode = e.getDestination().equals(crt) ? e.getSource() : e.getDestination();
					if (!visited.contains(nextNode)) {
						queue.add(nextNode);
						temp *= ((BigMultigraph)graph).getLabelFreq().get(e.getLabel()).getFrequency() / (double)graph.edgeSet().size();
					}
				}
				for (Edge e : query.incomingEdgesOf(crt)) {
					Long nextNode = e.getDestination().equals(crt) ? e.getSource() : e.getDestination();
					if (!visited.contains(nextNode)) {
						queue.add(nextNode);
						temp *= ((BigMultigraph)graph).getLabelFreq().get(e.getLabel()).getFrequency() / (double)graph.edgeSet().size();
					}
				}
			}
			level++;
		}
		
		return num;
	}
	
	public void computeEdSeedSel(double baseSel,  Long crt, Set<Long> visited, int depth, int max) {
		if (depth >= max) {
			return;
		}
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
			return;
		}
		for (Edge e: adjEdges) {
			double labelSel = ((BigMultigraph)graph).getLabelFreq().get(e.getLabel()).getFrequency() / (double)graph.edgeSet().size();
			double nextSel = labelSel * seedSel;
			
			if (e.getDestination().equals(e.getSource())) {
//				nextSel = Math.min(baseSel, labelSel);
				continue;
			}
			Long nextNode = e.getDestination().equals(crt) ? e.getSource() : e.getDestination();
//			System.out.println(crt + " " + e.getLabel() + " " + nextNode +" " + labelSel + " " + nextSel + " " + adjEdges.size());
//			System.out.println(crt + " " + nextNode + " " + nextSel + " " + labelSel);
			computeSelPathNotCorrelated(nextSel, nextNode, visited, depth + 1, max);
		}
	}
	
	public double computeSelPathNotCorrelated(double baseSel,  Long crt, Set<Long> visited, int depth, int max) {
		if (depth >= max) {
			return baseSel;
		}
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
		double sel = 1;
		for (Edge e: adjEdges) {
			double labelSel = ((BigMultigraph)graph).getLabelFreq().get(e.getLabel()).getFrequency() / (double)graph.edgeSet().size();
			double nextSel = Math.min(labelSel, baseSel);
			
			if (e.getDestination().equals(e.getSource())) {
//				nextSel = Math.min(baseSel, labelSel);
				continue;
			}
			Long nextNode = e.getDestination().equals(crt) ? e.getSource() : e.getDestination();
//			System.out.println(crt + " " + e.getLabel() + " " + nextNode +" " + labelSel + " " + nextSel + " " + adjEdges.size());
//			System.out.println(crt + " " + nextNode + " " + nextSel + " " + labelSel);
			sel *= computeSelPathNotCorrelated(nextSel, nextNode, visited, depth + 1, max);
		}
		return sel;
	}

}
