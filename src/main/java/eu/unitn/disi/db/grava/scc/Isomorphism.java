package eu.unitn.disi.db.grava.scc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javafx.util.Pair;
import eu.unitn.disi.db.grava.graphs.Answer;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.GraphRing;
import eu.unitn.disi.db.grava.graphs.MappedEdge;
import eu.unitn.disi.db.grava.graphs.MappedNode;
import eu.unitn.disi.db.grava.graphs.Multigraph;

public class Isomorphism {
	private Map<Edge, Set<MappedEdge>> queryGraphEdges;
	private int threshold;
	private Collection<Edge> queryEdges;
	private int count;
	private Answer a;
	private Multigraph query;
	private Long startingNode;
	private Map<Long, Set<MappedNode>> queryGraphMapping;
	private int queryNodesNum;
	private LinkedList<Long> prevQueryNodes;
	private LinkedList<Long> searchList;
	private LinkedList<Edge> solutionEdges;
	private int mnStartingIndex;
	private ArrayList<Long> visitedQueryNodes;
	private HashMap<Long, Long> currentMapping;
	private HashSet<GraphRing> queryRingNodes;
	private LinkedList<GraphRing> graphRingNodes;
	private BufferedWriter resultsWriter;
	private String anwserFile;
	private String outputDir;
	private String graphName;
	private HashSet<Answer> answers;

	public Isomorphism() {
		queryGraphEdges = new HashMap<>();
		count = 0;
		a = new Answer();
		startingNode = null;
		outputDir = null;
		mnStartingIndex = 0;
		answers = new HashSet<>();
	}

	public void findIsomorphism() throws IOException {
		Collection<Long> queryNodes = query.vertexSet();
		queryNodesNum = queryNodes.size();
		solutionEdges = new LinkedList<Edge>();
		prevQueryNodes = new LinkedList<Long>();
		currentMapping = new HashMap<Long, Long>();
		visitedQueryNodes = new ArrayList<Long>();
		queryRingNodes = new HashSet<GraphRing>();
		graphRingNodes = new LinkedList<GraphRing>();
		outputDir = graphName + "_output";

		File dir = new File(outputDir);
		if (!dir.exists()) {
			if (dir.mkdir()) {
				System.out.println("Creating directory succees");
			} else {
				System.err.println("creating directory fail");
			}
		}
		File file = new File(outputDir + "/"
				+ this.concateAnswerFile(anwserFile));
		if (!file.exists()) {
			file.createNewFile();
		}
		resultsWriter = new BufferedWriter((new OutputStreamWriter(
				new FileOutputStream(file, true))));

		if (startingNode == null) {
			startingNode = queryNodes.iterator().next();
		}
		Set<Long> visitedQueryNodes = new HashSet<Long>();

		searchList = this.computeSearchList(startingNode);
//		System.out.println("search list");
//		for (Long temp : searchList) {
//			System.out.println(temp);
//		}
//		System.out.println("prev list");
//		for (Long temp : prevQueryNodes) {
//			System.out.println(temp);
//		}
		this.combineMappedNodes(0, this.threshold);
		this.writeToFile();
		System.out.println("total answer number:" + count);
	}

	private void combineMappedNodes(int index, int remainingThreshold)
			throws IOException {
		Long currentQueryNode = searchList.get(index);
		Set<MappedNode> mappedNodes = queryGraphMapping.get(currentQueryNode);
		boolean isPrintingSolution = false;
		int ringStatus = 0;
		Long currentNodesPrev;

		int nextRemainingThreshold = remainingThreshold;
		long ringNode = 0L;
		if (index > 0) {
			Long prevForCurrentQNode = prevQueryNodes.get(index);
			Long prevForPrevQNode = prevQueryNodes.get(index - 1);
			if (queryRingNodes.contains(new GraphRing(searchList.get(index),
					prevQueryNodes.get(index)))) {
				ringStatus++;
			}

		}

		if (index == searchList.size() - 1) {
			isPrintingSolution = true;
		}

		for (MappedNode mn : mappedNodes) {
			Edge mappedEdge = mn.getMappedEdge();
			if (mappedEdge == null) {
				currentMapping.put(currentQueryNode, mn.getNodeID());
				// currentNodesPrev = mn.getNodeID();
				this.combineMappedNodes(index + 1, nextRemainingThreshold);
			} else {
				if (solutionEdges.contains(mappedEdge)) {
					continue;
				} else {
					currentNodesPrev = mn.isIncoming() ? mappedEdge.getSource()
							: mappedEdge.getDestination();
					
					if (!prevQueryNodes.get(index).equals(currentQueryNode)
							&& mappedEdge.getSource().equals(
									mappedEdge.getDestination())) {
						// remove self ring
						continue;
					}
					if(ringStatus == 0 && graphRingNodes.contains(new GraphRing(mappedEdge.getSource(),mappedEdge.getDestination()))){
						//if query nodes are  not a ring  and graph nodes are ring
						System.out.println(mappedEdge.getSource() + " " + mappedEdge.getDestination());
						continue;
					}
					if (ringStatus == 2 && mn.getNodeID() != ringNode) {
						// if it's a ring, it should have same ring node as
						// previous mapping
						continue;
					}
					

					if (mn.isLabelDif() && nextRemainingThreshold == 0) {
						continue;
					} else {
						Long prevMappedNode = currentMapping.get(prevQueryNodes
								.get(index));
						if (currentNodesPrev.equals(prevMappedNode)) {
							solutionEdges.addLast(mappedEdge);
							graphRingNodes.addLast(new GraphRing(mappedEdge.getSource(),mappedEdge.getDestination()));
							if (!isPrintingSolution) {
								
								currentMapping.put(currentQueryNode,
										mn.getNodeID());
								if (mn.isLabelDif()) {
									nextRemainingThreshold = remainingThreshold - 1;
								}
								if (ringStatus == 1) {
									ringStatus++;
									ringNode = mn.getNodeID();
									continue;
								} else {
									this.combineMappedNodes(index+1,
											nextRemainingThreshold);
								}
							} else {
								// this.printSolution(solutionEdges);
								if (ringStatus == 1) {
									ringStatus++;
									ringNode = mn.getNodeID();
									continue;
								} else {
									this.addToAnswers(solutionEdges);
									count++;
								}
							}
							if (ringStatus == 2) {
								graphRingNodes.pollLast();
								solutionEdges.pollLast();
							}
							graphRingNodes.pollLast();
							solutionEdges.pollLast();
						}
					}
				}
			}
		}

	}

	private void writeToFile() throws IOException {
		int i = 0;
		for (Answer ans : answers) {
			resultsWriter.write("query solution " + i);
			resultsWriter.newLine();
			resultsWriter.write(ans.toString());
			i++;
		}
	}

	private void computeRelatedQuery(Long queryNode, Long graphNode) {
		visitedQueryNodes.add(queryNode);
		System.out.println("visiting:" + queryNode);
		Collection<Edge> incomingEdges = query.incomingEdgesOf(queryNode);
		Long nextNode;

		Set<MappedNode> mappedNodes = null;

		for (Edge e : incomingEdges) {
			nextNode = e.getSource();
			if (visitedQueryNodes.contains(nextNode)) {
				continue;
			} else {
				mappedNodes = queryGraphMapping.get(nextNode);
				for (MappedNode mn : mappedNodes) {
					if (!mn.isIncoming()) {
						this.computeRelatedQuery(nextNode, mn.getNodeID());
					}
				}
			}
		}
		Collection<Edge> outgoingEdges = query.outgoingEdgesOf(queryNode);
		for (Edge e : outgoingEdges) {
			nextNode = e.getDestination();
			if (visitedQueryNodes.contains(nextNode)) {
				continue;
			} else {
				for (MappedNode mn : mappedNodes) {
					this.computeRelatedQuery(nextNode, mn.getNodeID());
				}
			}
		}

	}

	private String concateAnswerFile(String fileName) {
		String[] split = fileName.split("/");
		return graphName + "_results_" + split[split.length - 1];
	}

	private LinkedList<Long> computeSearchList(Long startingNode) {
		LinkedList<Long> searchList = new LinkedList<Long>();
		LinkedList<Long> temp = new LinkedList<Long>();
		ArrayList<Long> repeatedNodes = new ArrayList<Long>();
		prevQueryNodes.add(-1L);
		searchList.addLast(startingNode);
		temp.addLast(startingNode);
		Long nodeToVisit;
		while (searchList.size() < queryNodesNum) {
			repeatedNodes.clear();
			nodeToVisit = temp.pollFirst();
			Collection<Edge> incomingEdges = query.incomingEdgesOf(nodeToVisit);
			for (Edge e : incomingEdges) {
				repeatedNodes.add(e.getSource());
				if (searchList.contains(e.getSource())) {
					continue;
				} else {
					searchList.addLast(e.getSource());
					temp.addLast(e.getSource());
					prevQueryNodes.add(nodeToVisit);
				}
			}
			Collection<Edge> outgoingEdges = query.outgoingEdgesOf(nodeToVisit);
			for (Edge e : outgoingEdges) {
				if (repeatedNodes.contains(e.getDestination())) {
					// System.out.println(e.getSource() + " " +
					// e.getDestination());
					queryRingNodes.add(new GraphRing(e.getSource(), e
							.getDestination()));
				} else {
					repeatedNodes.add(e.getDestination());
				}
				if (searchList.contains(e.getDestination())) {
					continue;
				} else {
					searchList.addLast(e.getDestination());
					temp.addLast(e.getDestination());
					prevQueryNodes.add(nodeToVisit);
				}
			}
		}
		return searchList;
	}

	private void addToAnswers(LinkedList<Edge> edges) {
		Answer ans = new Answer();
		for (Edge e : edges) {
			ans.add(e.toString());
		}
		answers.add(ans);
	}

	private void printSolution(LinkedList<Edge> edges) throws IOException {
		resultsWriter.write("query solution " + count);
		resultsWriter.newLine();
		for (Edge e : edges) {
			resultsWriter.write(e.toString());
			resultsWriter.newLine();
		}

	}



	private void printAnswer(LinkedList<Edge> answer, Edge edge)
			throws IOException {
		resultsWriter.write("query solution " + count);
		resultsWriter.newLine();
		for (Edge e : answer) {
			resultsWriter.write(e.toString());
			resultsWriter.newLine();
		}
		resultsWriter.write(edge.toString());
		resultsWriter.newLine();

	}

	public void mappingEdges(Map<Long, Set<MappedNode>> queryGraphMapping) {
		long src;
		long des;
		Set<MappedNode> srcMappedNodes;
		Set<MappedNode> desMappedNodes;
		Set<MappedEdge> mappedEgdes = null;
		MappedEdge me;
		for (Edge e : this.queryEdges) {
			mappedEgdes = new HashSet<MappedEdge>();
			src = e.getSource();
			des = e.getDestination();
			srcMappedNodes = queryGraphMapping.get(src);
			desMappedNodes = queryGraphMapping.get(des);
			for (MappedNode srcMn : srcMappedNodes) {
				for (MappedNode desMn : desMappedNodes) {
					me = this.checkConnection(srcMn, desMn);
					if (me != null) {
						mappedEgdes.add(me);
					}
				}
			}
			queryGraphEdges.put(e, mappedEgdes);
		}
	}

	private MappedEdge checkConnection(MappedNode srcMn, MappedNode desMn) {
		long src = srcMn.getNodeID();
		long des = desMn.getNodeID();
		Edge temp = null;
		Edge result = null;
		int dis = -1;
		if (srcMn.getMappedEdge() == null && desMn.getMappedEdge() == null) {
			return null;
		} else if (srcMn.getMappedEdge() == null) {
			result = this.match(src, desMn);
			dis = desMn.getDist();
		} else if (desMn.getMappedEdge() == null) {
			result = this.match(des, srcMn);
			dis = srcMn.getDist();
		} else {
			if ((result = this.match(src, desMn)) == null) {
				result = this.match(des, srcMn);
				dis = srcMn.getDist();
			} else {
				dis = desMn.getDist();
			}
		}
		if (result != null) {
			return new MappedEdge(result, dis);
		} else {
			return null;
		}

	}

	private Edge match(long node, MappedNode mn) {
		Edge e = mn.getMappedEdge();
		Edge result = null;
		if (mn.isIncoming()) {
			if (e.getSource() == node) {
				result = mn.getMappedEdge();
			}
		} else {
			if (e.getDestination() == node) {
				result = mn.getMappedEdge();
			}
		}
		return result;
	}

	public Map<Edge, Set<MappedEdge>> getQueryGraphEdges() {
		return queryGraphEdges;
	}

	public void setQueryGraphEdges(Map<Edge, Set<MappedEdge>> queryGraphEdges) {
		this.queryGraphEdges = queryGraphEdges;
	}

	public int getThreshold() {
		return threshold;
	}

	public void setThreshold(int threshold) {
		this.threshold = threshold;
	}

	public Collection<Edge> getQueryEdges() {
		return queryEdges;
	}

	public void setQueryEdges(Collection<Edge> queryEdges) {
		this.queryEdges = queryEdges;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public Answer getA() {
		return a;
	}

	public void setA(Answer a) {
		this.a = a;
	}

	public Multigraph getQuery() {
		return query;
	}

	public void setQuery(Multigraph query) {
		this.query = query;
	}

	public Long getStartingNode() {
		return startingNode;
	}

	public void setStartingNode(Long startingNode) {
		this.startingNode = startingNode;
	}

	public Map<Long, Set<MappedNode>> getQueryGraphMapping() {
		return queryGraphMapping;
	}

	public void setQueryGraphMapping(
			Map<Long, Set<MappedNode>> queryGraphMapping) {
		this.queryGraphMapping = queryGraphMapping;
	}

	public String getAnwserFile() {
		return anwserFile;
	}

	public void setAnwserFile(String anwserFile) {
		this.anwserFile = anwserFile;
	}

	public String getOutputDir() {
		return outputDir;
	}

	public void setOutputDir(String outputDir) {
		this.outputDir = outputDir;
	}

	public String getGraphName() {
		return graphName;
	}

	public void setGraphName(String graphName) {
		this.graphName = graphName;
	}

	public BufferedWriter getResultsWriter() {
		return resultsWriter;
	}

	public void setResultsWriter(BufferedWriter resultsWriter) {
		this.resultsWriter = resultsWriter;
	}

	public HashSet<Answer> getAnswers() {
		return answers;
	}

	public void setAnswers(HashSet<Answer> answers) {
		this.answers = answers;
	}

}
