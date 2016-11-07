package eu.unitn.disi.db.grava.scc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Stack;

import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;

public class Sampling {
	private int maxDegree;
	private int maxNodesNum;
	private Long startingNode;
	private BigMultigraph G;
	private Queue<Long> queue;
	// private boolean[] marked;
	private Collection<Long> vertexSet;
	// private static HashMap<Integer, Long> IntToLong;
	// private static HashMap<Long, Integer> LongToInt;
	private int visitedNodesNum;
	private int numberOfNodes;
	private int count;
//	private Queue<Integer> distance;
	private HashSet<Long> visited;
	private int dis;
	private int pre;
	private double rate;
	private int ansNum;
	private int ans;

	public Sampling(BigMultigraph G) {

		this.G = G;
	}

	public Sampling(BigMultigraph G, int maxNodesNum, int maxDegree,
			Long startingNode, String fileName) throws IOException {
		this.G = G;
		numberOfNodes = G.numberOfNodes();
		visitedNodesNum = 0;
		vertexSet = G.vertexSet();
		// IntToLong = this.constructIntToLong(vertexSet);
		// LongToInt = this.constructLongToInt(vertexSet);
		queue = new LinkedList<Long>();
//		distance = new LinkedList<Integer>();
		visited = new HashSet<Long>();
		// marked = new boolean[G.numberOfNodes()];
		this.maxDegree = maxDegree;
		this.maxNodesNum = maxNodesNum;
		this.startingNode = startingNode;
		ansNum = 2;
		ans = 0;
		count = 0;
		dis = 0;
		pre = -1;
//		distance.add(dis);
		rate = 0.1;
		this.bfs(G, startingNode, fileName);
	}

	public Long Int2Long(int in) {
		int i = 0;
		for (Long temp : vertexSet) {
			if (i == in) {
				return temp;
			}
			i++;
		}
		return (long) -1;

	}

	public int Long2Int(Long l) {
		int i = 0;
		for (Long temp : vertexSet) {
			if (temp.equals(l)) {
				return i;
			}
			i++;
		}
		return -1;

	}

	private void bfs(BigMultigraph G, Long v, String fileName) {
		// marked[v] = true;
		// System.out.println("bfs: " + v);
		// Long test = queue.poll();
		//
		// if(test.equals(v)){
		// System.out.println("Succeed");
		// }else{
		// System.out.println("Fail");
		// }
		// long temp = Int2Long(v);
		// System.out.println("long:" + temp);
		
		queue.add(v);
		count++;
		Collection<Edge> adjEdges = null;
		Long top;
		Long newNode;
		Long prevNode;
		int degree;
		int cur;
		boolean flag = true;
		double limit = 0.05;
		double crt = 0;
//		List<String> results = new ArrayList<>();
		File out = new File(fileName);
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(out, true));
		while (flag && !queue.isEmpty()) {
			top = queue.poll();
//			cur = distance.poll();
			
			if (visited.contains(top)) {
				continue;
			}
			visited.add(top);
			count++;
			if (count % 100 == 0) System.out.println("Processed nodes " + count );
//			System.out.println("current visiting:" + top);
			adjEdges = G.adjEdges(top);
			// System.out.println(adjEdges.size());
			degree = 0;
//			if (cur != pre) {
//				dis++;
//				pre = cur;
//				rate *= 1 / Math.exp(dis);
//				// System.out.println("distance " + cur +" rate " + rate);
//			}
			for (Edge e : adjEdges) {
				// System.out.println("Count:" + count);
				// System.out.println("degree:" + degree);
//				double sel = ((double)G.getLabelFreq().get(e.getLabel()).getFrequency()) / G.edgeSet().size();
//				if (sel + crt > limit) {
//					continue;
//				}
//				crt += sel;
				if (count > this.maxNodesNum - 1) {
					flag = false;
					System.out.println("maximum nodes num");
					break;
				}
				if (degree > this.maxDegree - 1) {
					System.out.println("maximum degree exit");
					break;
				}
				newNode = (e.getSource().equals(top)) ? e.getDestination() : e
						.getSource();
				// System.out.println("new Nodes" + newNode);
				if (!visited.contains(newNode)) {
					queue.add(newNode);
//					distance.add(dis);
					// System.out.println(e.getSource() + " " +
					// e.getDestination() + " " + e.getLabel());
					// bw.write(e.getSource() + " " + e.getDestination() + " " +
					// e.getLabel());
//					System.out.println(newNode);
//					System.out.println(e.getSource() + " " + e.getDestination()
//							+ " " + e.getLabel());
					bw.write(e.getSource() + " " + e.getDestination() + " "
							+ e.getLabel());
					bw.newLine();
					// if(ans <= ansNum && Math.random() <= rate){
					//
					// ans++;
					// bw.write(e.getDestination() + " " +
					// "5378845548 1000014827");
					// bw.newLine();
					// bw.write("5378845548 828277504948 1000008783");
					// bw.newLine();
					// bw.write("828277504948 82878277755348 1000008725");
					// bw.newLine();
					// bw.write("82878277755348 5378817448 1000008783");
					// bw.newLine();
					// System.out.println("current answer number:" + ans + "/" +
					// ansNum);
					// // System.out.println("printing " + rate + " " + dis);
					// }
					degree++; // out degree
				}

			}

			// System.out.println("rate:" + rate);

		}
		
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} finally {
			try {
				bw.flush();
				bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
//		if (results.size() >= 3) {
//		File out = new File(fileName);
//		BufferedWriter bw = new BufferedWriter(new FileWriter(out));
//		for (String str : results) {
//			bw.write(str);
//			bw.newLine();
//		}
//		bw.close();
//		}
//		System.out.println("crt fre:" + crt);

		// if(count > this.maxNodesNum){
		// System.out.println("Exceed the maximum nodes number");
		// return;
		// }
		// if(adjNodes != null){
		// System.out.println("adjNodes size:" + adjNodes.size());
		// int degree = 0;
		// for (Edge e : adjNodes) {
		// Long w = (e.getSource() == v)?e.getSource():e.getDestination();
		// degree++;
		// if(degree > this.maxDegree){
		// System.out.println("Exceed the maximum degree");
		// break;
		// }
		// int intW = Long2Int(w);
		// if (!marked[intW]) {
		// System.out.println(e.getSource() + " " + e.getDestination() + " " +
		// e.getLabel());
		// queue.add(intW);
		// bfs(G, intW);
		// }
		// }
		// }else{
		// return;
		// }
	}

	public HashMap<Integer, Long> constructIntToLong(Collection<Long> vertexSet) {
		HashMap<Integer, Long> mapping = new HashMap<Integer, Long>();
		int i = 0;
		for (Long temp : vertexSet) {
			mapping.put(i, temp);
		}
		return mapping;
	}

	public HashMap<Long, Integer> constructLongToInt(Collection<Long> vertexSet) {
		HashMap<Long, Integer> mapping = new HashMap<Long, Integer>();
		int i = 0;
		for (Long temp : vertexSet) {
			mapping.put(temp, i);
		}
		return mapping;
	}

	// generate path queries and fan-shaped queries with same label
	private void generateQuery(int edgeNum) throws FileNotFoundException,
			UnsupportedEncodingException {
		int count = 0;
		Collection<Long> nodes = G.vertexSet();
		Collection<Edge> edges;
		int j;
		long currentNode;
		int en = edgeNum;
		edgeNum--;
		PrintWriter w = null;
		PrintWriter fw = null;
		Long prevLabel = null;
		for (Long node : nodes) {
			edges = G.outgoingEdgesOf(node);
			currentNode = node;
			for (Edge e : edges) {
				if (count >= 300) {
					return;
				}
				if (prevLabel != null && prevLabel.equals(e.getLabel())) {
					continue;
				} else {
					prevLabel = e.getLabel();
				}
				List<ArrayList<Edge>> results = new ArrayList<ArrayList<Edge>>();
				List<Edge> currentEdges = new ArrayList<Edge>();
				currentEdges.add(e);

				currentNode = ((node.equals(e.getDestination())) ? e
						.getSource() : e.getDestination());
				ArrayList<Long> usedNodes = new ArrayList<Long>();
				usedNodes.add(currentNode);
				this.dfs(results, edges, currentEdges, currentNode, edgeNum,
						usedNodes);
				for (ArrayList<Edge> result : results) {
					w = new PrintWriter("./test/E" + (en) + "PQ" + count
							+ ".txt", "UTF-8");
					System.out.println(node);
					System.out.println("path query " + count);
					for (Edge r : result) {
						w.write(r.getSource() + " " + r.getDestination() + " "
								+ r.getLabel());
						w.write("\n");
						System.out.println(r);
					}
					fw = new PrintWriter("./test/E" + (en) + "FQ" + count
							+ ".txt", "UTF-8");
					System.out.println("fan query:" + count);
					for (int m = 0; m < result.size(); m++) {
						int repeat = 0;
						for (int n = 0; n < m; n++) {
							if (result.get(n).getLabel()
									.equals(result.get(m).getLabel())) {
								repeat++;
							}
						}
						Edge r = result.get(m);
						System.out.println(repeat);
						Edge out = this.getEdgeWithLabel(edges, r.getLabel(),
								repeat);
						fw.write(out.getSource() + " " + out.getDestination()
								+ " " + out.getLabel());
						fw.write("\n");
					}
					w.close();
					fw.close();
					w = null;
					fw = null;
					count++;
				}
			}
		}

	}

	// generate path query and fan shaped query
	private void randomlyGenerateQuery(int edgeNum)
			throws FileNotFoundException, UnsupportedEncodingException {
		int pCount = 0, fCount = 0;
		Collection<Long> nodes = G.vertexSet();
		Collection<Edge> edges;
		int j;
		long currentNode;
		int en = edgeNum;
		edgeNum--;
		PrintWriter w = null;
		PrintWriter fw = null;
		Long prevLabel = null;
		for (Long node : nodes) {
			edges = G.outgoingEdgesOf(node);
			currentNode = node;
			for (Edge e : edges) {
//				if (((BigMultigraph) G).getLabelFreq().get(e.getLabel())
//						.getFrequency() > 100) {
//					continue;
//				}
				if (pCount >= 100) {
					break;
				}

				List<ArrayList<Edge>> results = new ArrayList<ArrayList<Edge>>();
				List<Edge> currentEdges = new ArrayList<Edge>();
				currentEdges.add(e);

				currentNode = ((node.equals(e.getDestination())) ? e
						.getSource() : e.getDestination());
				ArrayList<Long> usedNodes = new ArrayList<Long>();
				usedNodes.add(currentNode);
				this.dfs(results, edges, currentEdges, currentNode, edgeNum,
						usedNodes);
				for (ArrayList<Edge> result : results) {
					w = new PrintWriter("./test/PQ" + en +"E/E" + (en) + "PQ" + pCount
							+ ".txt", "UTF-8");
					System.out.println(node);
					System.out.println("path query " + pCount);
					for (Edge r : result) {
						w.write(r.getSource() + " " + r.getDestination() + " "
								+ r.getLabel());
						w.write("\n");
						System.out.println(r);
					}
					// fw = new PrintWriter("./test/E"+(en) + "FQ"+count +
					// ".txt", "UTF-8");
					// System.out.println("fan query:" + count);
					// for(int m = 0; m < result.size(); m++){
					// int repeat = 0;
					// for(int n = 0; n < m; n++){
					// if(result.get(n).getLabel().equals(result.get(m).getLabel())){
					// repeat++;
					// }
					// }
					// Edge r = result.get(m);
					// System.out.println(repeat);
					// Edge out = this.getEdgeWithLabel(edges, r.getLabel(),
					// repeat);
					// fw.write(out.getSource() + " " + out.getDestination() +
					// " " + out.getLabel());
					// fw.write("\n");
					// }
					w.flush();
					w.close();
					// fw.close();
					w = null;
					// fw = null;
					pCount++;
				}
			}
			if (edges.size() >= en && fCount < 100) {

				
				int i = 0;
				for (Edge e : edges) {
//					if (((BigMultigraph) G).getLabelFreq().get(e.getLabel())
//							.getFrequency() > 100) {
//						continue;
//					}
					// fw.write(e.getSource() + " " + e.getDestination() + " "
					// + e.getLabel());
					// fw.write("\n");
					i++;
					System.out.println(i + " " + en);
				}

				if (i >= en) {
					i = 0;
					
					fw = new PrintWriter(
							"./test/FQ" + en + "E/E" + (en) + "FQ" + fCount + ".txt", "UTF-8");
					for (Edge e : edges) {
//						if (((BigMultigraph) G).getLabelFreq()
//								.get(e.getLabel()).getFrequency() > 100) {
//							continue;
//						}
						if (i < en) {
							 fw.write(e.getSource() + " " + e.getDestination()
							 + " "
							 + e.getLabel());
							 fw.write("\n");
							 System.out.println(i + " " + e);
							i++;
						} else {
							break;
						}
					}
					fCount++;
					fw.flush();
					fw.close();
				}
				
				fw = null;
				

			}
		}

	}
	
	private void generateQueries(double limit, int queriesNum, String folder) {
		File ff = new File(folder);
		if (!ff.exists()) {
			ff.mkdir();
		}
		Collection<Long> nodes = G.vertexSet();
		int count = 0;
		Random rnd = new Random();
		for (Long node : nodes) {
			if (rnd.nextFloat() < 0.2) continue;
			if (count > queriesNum) break;
			try {
				List<Long> newNodes = new ArrayList<>();
				newNodes.add(node);
				if (this.generateQueriesWittLimits(folder, newNodes, new ArrayList<>(), 1, limit, count)) {
					count++;
				}
			} catch (FileNotFoundException | UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	private boolean generateQueriesWittLimits(String folder, List<Long> nodes,
			List<Edge> edges, double crt, double limit, int count) throws FileNotFoundException,
			UnsupportedEncodingException {
		if (crt <= limit) {
			if (edges.size() < 4) return false;
			PrintWriter fw = new PrintWriter(folder + count + ".txt", "UTF-8");
			for (Edge e : edges) {
				fw.write(e.getSource() + " " + e.getDestination() + " "
						+ e.getLabel() + "\n");
			}
			fw.close();
			return true;
		}

		double choice = Math.random();
			// System.out.println(choice);
		int nodeIdx = (int) (choice * nodes.size());
		Long node = nodes.get(nodeIdx);
		System.out.println("crt " + crt + " " + limit);
//		System.out.println(nodeIdx + " " + nodes.size());
		Collection<Edge> oes = G.outgoingEdgesOf(node);
		double prob = 1 / (double) oes.size();
		double next = crt;
		for (Edge e : oes) {
			double sel = ((BigMultigraph) G).getLabelFreq().get(e.getLabel()).getFrequency() / (double)G.edgeSet().size();
			
			if (!edges.contains(e) && !e.getDestination().equals(node)) {
				edges.add(e);
				System.out.println(e);
				nodes.add(e.getDestination());
				next *= sel;
				break;
			}
		}
		if (crt == next) {
			oes = G.incomingEdgesOf(node);
			for (Edge e : oes) {
				double sel = ((BigMultigraph) G).getLabelFreq().get(e.getLabel()).getFrequency() / (double)G.edgeSet().size();
				
				if (!edges.contains(e) && !e.getDestination().equals(node)) {
					edges.add(e);
//					System.out.println(e);
					nodes.add(e.getDestination());
					next *= sel;
					break;
				}
			}
			if (crt == next) return false;
		}
		return generateQueriesWittLimits(folder, nodes, edges, next, limit, count);
	}
	
	private void generateQueries(int edgeNum, int queriesNum) {
		Collection<Long> nodes = G.vertexSet();
		int count = 0;
		for (Long node : nodes) {
			if (count > queriesNum)
				break;
//				for (int i = 2; i <= edgeNum; i++) {
					try {
						this.generateDifSizeQueries(6, node);
					} catch (FileNotFoundException
							| UnsupportedEncodingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					count++;
//				}
		}
	}

	private void generateDifSizeQueries(int edgeNum, Long node)
			throws FileNotFoundException, UnsupportedEncodingException {
		List<Long> nodes = new ArrayList<>();
		nodes.add(node);
		generateQueriesWithAtMost(edgeNum, nodes, new ArrayList<Edge>(), 0, 0);
	}

	private void generateQueriesWithAtMost(int edgeNum, List<Long> nodes,
			List<Edge> edges, int depth, double crt) throws FileNotFoundException,
			UnsupportedEncodingException {
		if (depth > edges.size() * 10 + 10) {
			return;
		}
		if (edgeNum == 0) {
//			File f = new File("./test/test/E1E/");
//			if (!f.exists()) {
//				f.mkdirs();
//			}
			PrintWriter fw = new PrintWriter("./queryFolder/10000nodes/eval/" + "E" + edges.size() + "E"
					+ nodes.get(0) + ".txt", "UTF-8");
			for (Edge e : edges) {
				fw.write(e.getSource() + " " + e.getDestination() + " "
						+ e.getLabel() + "\n");
			}
			fw.close();
		} else {
			double choice = Math.random();
			// System.out.println(choice);
			int nodeIdx = (int) (choice * nodes.size());
			Long node = nodes.get(nodeIdx);
			System.out.println(nodeIdx + " " + nodes.size());
			Collection<Edge> oes = G.outgoingEdgesOf(node);
			int edgeSize = oes.size();
			double prob = 1 / (double) oes.size();
			for (Edge e : oes) {
//				if (((BigMultigraph) G).getLabelFreq().get(e.getLabel())
//						.getFrequency() > 150) {
//					continue;
//				}
				double sel = ((BigMultigraph) G).getLabelFreq().get(e.getLabel()).getFrequency() / (double)G.vertexSet().size();
				if ( !edges.contains(e) && crt + sel<= 0.05) {
					edgeNum--;
					edges.add(e);
					nodes.add(e.getDestination());
					crt += sel;
					break;
				}
			}
			generateQueriesWithAtMost(edgeNum, nodes, edges, depth + 1, crt);
		}
	}

	private void generateQueriesWithAtMost(int edgeNum, int totalQueryNum)
			throws FileNotFoundException, UnsupportedEncodingException {
		int pCount = 0, fCount = 0;
		Collection<Long> nodes = G.vertexSet();
		Collection<Edge> edges;
		long currentNode;
		int en = edgeNum;
		PrintWriter w = null;
		PrintWriter fw = null;
		Long prevLabel = null;
		for (Long node : nodes) {
			edges = G.outgoingEdgesOf(node);
			if (edges.size() >= edgeNum) {
				for (int i = 2; i <= edgeNum; i++) {
					fw = new PrintWriter("./test/FQN" + node + "E" + (i)
							+ ".txt", "UTF-8");
					int j = 0;
					for (Edge e : edges) {
						if (j < i) {
							if (fCount <= totalQueryNum) {
								fw.write(e.getSource() + " "
										+ e.getDestination() + " "
										+ e.getLabel());
								fw.write("\n");
							} else {
								return;
							}
						} else {
							break;
						}
						j++;
					}
					fw.close();
					fw = null;
				}
				fCount++;
			}

		}

	}

	private void writeCliqueToFile(List<Long> prevNodes)
			throws FileNotFoundException, UnsupportedEncodingException {
		Collection<Edge> ins = null;
		Collection<Edge> outs = null;
		Long tempNode = null;
		Long firstNode = null;
		Long secondNode = null;
		PrintWriter w = new PrintWriter("./test/Clique" + count + ".txt",
				"UTF-8");
		;
		for (int i = 0; i < prevNodes.size(); i++) {
			for (int j = i + 1; j < prevNodes.size(); j++) {
				firstNode = prevNodes.get(i);
				secondNode = prevNodes.get(j);
				ins = G.incomingEdgesOf(firstNode);
				for (Edge in : ins) {
					tempNode = in.getDestination().equals(firstNode) ? in
							.getSource() : in.getDestination();
					if (tempNode.equals(secondNode)) {
						w.write(in.getSource() + " " + in.getDestination()
								+ " " + in.getLabel());
						w.write("\n");
						System.out.println(in);
					}
				}

				outs = G.outgoingEdgesOf(firstNode);
				for (Edge out : outs) {
					tempNode = out.getDestination().equals(firstNode) ? out
							.getSource() : out.getDestination();
					if (tempNode.equals(secondNode)) {
						w.write(out.getSource() + " " + out.getDestination()
								+ " " + out.getLabel());
						w.write("\n");
						System.out.println(out);
					}
				}
			}

		}
		w.close();
	}

	private void generateClique(int nodeNum, int index, List<Long> prevNodes) {
		if (nodeNum == 0) {
			count++;
			if (count >= 300) {
				return;
			}
			System.out.println("=========" + count + "========");
			try {
				this.writeCliqueToFile(prevNodes);
			} catch (FileNotFoundException | UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		Collection<Long> nodes = G.vertexSet();
		int i = 0;
		boolean isAdding = true;
		for (Long node : nodes) {
			// System.out.println("processing:" + node);
			isAdding = true;
			if (i < index) {
				i++;
				continue;
			} else {
				i++;
				for (Long prevNode : prevNodes) {
					isAdding = isAdding && isConnected(node, prevNode);
				}
				if (isAdding) {
					prevNodes.add(node);
					generateClique(nodeNum - 1, i + 1, prevNodes);
					prevNodes.remove(prevNodes.size() - 1);
				}
			}

		}

	}

	private boolean isConnected(Long firstNode, Long secondNode) {
		Collection<Edge> ins = G.incomingEdgesOf(firstNode);
		Long tempNode = null;
		for (Edge in : ins) {
			tempNode = in.getDestination().equals(firstNode) ? in.getSource()
					: in.getDestination();
			if (tempNode.equals(secondNode)) {
				return true;
			}
		}

		Collection<Edge> outs = G.outgoingEdgesOf(firstNode);
		for (Edge out : outs) {
			tempNode = out.getDestination().equals(firstNode) ? out.getSource()
					: out.getDestination();
			if (tempNode.equals(secondNode)) {
				return true;
			}
		}
		return false;

	}

	private void dfs(List<ArrayList<Edge>> results, Collection<Edge> adjEdges,
			List<Edge> currentEdges, Long currentNode, int edgeNum,
			ArrayList<Long> usedNodes) {
		if (results.size() == 1) {
			return;
		}

		if (edgeNum == 0) {
			ArrayList<Edge> result = new ArrayList<Edge>();
			for (int i = 0; i < currentEdges.size(); i++) {
				result.add(currentEdges.get(i));
			}
			results.add(result);

		} else {
			edgeNum--;
			Collection<Edge> currentAdj = G.outgoingEdgesOf(currentNode);
			boolean nextRecur;
			Long tempNode = currentNode;
			for (Edge e : currentAdj) {
//				if (((BigMultigraph) G).getLabelFreq().get(e.getLabel())
//						.getFrequency() > 100) {
//					continue;
//				}
				if (e.equals(currentEdges.get(currentEdges.size() - 1))) {
					continue;
				}
				currentNode = (tempNode == e.getDestination()) ? e.getSource()
						: e.getDestination();
				if (usedNodes.contains(currentNode)) {
					continue;
				} else {
					usedNodes.add(currentNode);
				}
				nextRecur = false;
				if (this.isContainLabel(adjEdges, e.getLabel())) {
					if (!this.isContainLabel(currentEdges, e.getLabel())) {
						currentEdges.add(e);
						nextRecur = true;
					} else {
						int count = 0;
						for (int j = 0; j < currentEdges.size(); j++) {
							if (currentEdges.get(j).getLabel()
									.equals(e.getLabel())) {
								count++;
							}
						}
						for (Edge adjE : adjEdges) {
							if (adjE.getLabel().equals(e.getLabel())) {
								count--;
								if (count < 0) {
									currentEdges.add(e);
									nextRecur = true;
									break;
								}
							}
						}

					}
					if (nextRecur) {

						dfs(results, adjEdges, currentEdges, currentNode,
								edgeNum, usedNodes);
						currentEdges.remove(currentEdges.size() - 1);
					}

				} else {
					continue;
				}

			}
		}
	}

	private boolean isContainLabel(Collection<Edge> adjEdges, Long label) {
		for (Edge e : adjEdges) {
			if (e.getLabel().equals(label)) {
				return true;
			}
		}
		return false;
	}

	private Edge getEdgeWithLabel(Collection<Edge> adjEdges, Long label,
			int repeat) {
		for (Edge e : adjEdges) {
			if (e.getLabel().equals(label)) {
				if (repeat == 0) {
					return e;
				} else {
					repeat--;
				}
			}
		}
		return null;
	}

	public static void main(String[] args) throws ParseException, IOException {
		String graph = args[0];
		BigMultigraph G = new BigMultigraph(graph + "-sin.graph",
				graph + "-sout.graph", false);
//		int k = 20;
//		int maxNodes = 1000000;
//		int maxDegree = 100000;
//		Long node = 67678472568248L;
//		Sampling s = new Sampling(G, maxNodes, maxDegree, node, "1M.txt");
//		int size = G.vertexSet().size();
//		Random rnd = new Random();
//		Long[] nodes = G.vertexSet().toArray(new Long[size]);
//		for (int i = 0; i < k; i++) {
//			System.out.println(rnd.nextInt());
//			Long node = nodes[rnd.nextInt(size)];
//			System.out.println(node);
//			Sampling s = new Sampling(G, maxNodes, maxDegree, node, "queryFolder/100000nodes/" + i + ".txt");
//		}
//		int size = G.vertexSet().size();
//		Random rnd = new Random();
//		Long[] nodes = G.vertexSet().toArray(new Long[size]);
//		Sampling s = new Sampling(G);
//		for (int i = 2; i <= 10; i++){
//			s.randomlyGenerateQuery(i);
//		}
//		 for (int i = 0; i < 10; i++) {
//		 System.out.println("Starting node " + nodes[rnd.nextInt(size)]);
//		 for (int j = 2; j <= 5; j++) {
//			for (int k = 1; k <= 5; k++) {
//				 Sampling s = new Sampling(G, j * 10, k,
//				 nodes[rnd.nextInt(size)], "Q" + j * 10 + "N" + k
//				 + "D_" + (i + 1) + ".txt");
//				 }
//		 	}
//		 }
		 Sampling s = new Sampling(G);
//		 double limit = 0.0001 / G.edgeSet().size();
//		 System.out.println(limit);
//		 s.generateQueries(limit, 20, "./queryFolder/" + graph + "/limit/");
		 s.generateQueries(6, 100);
		// s.generateClique(3, 0, new ArrayList<Long>());
	}

}
