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
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.EdgeLabel;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.graphs.PathNeighbor;
import eu.unitn.disi.db.grava.vectorization.MemoryNeighborTables;
import eu.unitn.disi.db.grava.vectorization.NeighborTables;
import eu.unitn.disi.db.grava.vectorization.PathNeighborTables;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Compute tables in memory
 * 
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class ComputePathGraphNeighbors extends Algorithm {
	@AlgorithmInput
	private int k;
	@AlgorithmInput
	private Multigraph graph;
	@AlgorithmInput
	private int numThreads;
	@AlgorithmInput
	private Collection<Long> nodeProcessed = null;

	@AlgorithmOutput
	private PathNeighborTables neighborTables;

	private boolean debugThreads = false;

	// private int tid;

	private class ComputePathNodeNeighbors implements Callable<PathNeighborTables> {
		private final Long[] graphNodes;
		private final int id;
		private final int start;
		private final int end;

		public ComputePathNodeNeighbors(int id, Long[] graphNodes, int start,
				int end) {
			this.graphNodes = graphNodes;
			this.id = id;
			this.start = start;
			this.end = end;
		}

		@Override
		public PathNeighborTables call() throws Exception {
			PathNeighborTables tables = new PathNeighborTables(k);
			Set<Long> nextLevelToSee;
			long label, nodeToAdd;
			long count = 0L;
			short in;
			Integer countNeighbors;
			Set<Long> visited, toVisit, labels;
			Set<PathNeighbor> pn;

			Long node;
			Map<PathNeighbor, Integer> levelTable, lastLevelTable;
			Map<Long, Set<PathNeighbor>> lastLevelPath;
			Set<PathNeighbor> currentPath;
			Collection<Edge> inOutEdges;
			

//			debug("[T%d] Table computation started with %d nodes to process",id, end - start);
			for (int i = start; i < end && i < graphNodes.length; i++) {
				node = graphNodes[i];
//				if(node.equals(52665748L)){
//					System.out.println();
//				}
				toVisit = new HashSet<>();
				
				toVisit.add(node);
				visited = new HashSet<>();
				lastLevelTable = new HashMap<>();
				
				for (short l = 0; l < k; l++) {
					lastLevelPath = new HashMap<>();
					levelTable = new HashMap<>();
					
					nextLevelToSee = new HashSet<>();
					for (Long current : toVisit) {
						
						currentPath = new HashSet<>();
						// current = toVisit.poll();
						
						if (current == null) {
							warn("[T%d] NodeToExplore is null for level %d and node %d",
									id, l, current);
						}
						for (in = 0; in < 2; in++) { // Cycles over incoming and
														// outgoing
							inOutEdges = in == 0 ? graph
									.incomingEdgesOf(current) : graph
									.outgoingEdgesOf(current);
							if (inOutEdges != null) {
								for (Edge edge : inOutEdges) {
									label = edge.getLabel();
									
									nodeToAdd = in == 0 ? edge.getSource()
											: edge.getDestination();
									if (!visited.contains(nodeToAdd)) {
										
										if((pn = lastLevelPath.get(current)) == null){
											pn = new HashSet<>();
											PathNeighbor newPn = new PathNeighbor();
											newPn.add(new EdgeLabel(label, in == 0));
											pn.add(newPn);
											
										}else{
											pn = new HashSet<>(pn);
											Iterator<PathNeighbor> it = pn.iterator();
											while(it.hasNext()){
												PathNeighbor newPn = it.next();
												newPn.add(new EdgeLabel(label, in == 0));
												pn.add(newPn);
//												pn.remove(it);
											}
										}
										
										for(PathNeighbor pnTemp : pn){
											countNeighbors = levelTable.get(pnTemp);
											if (countNeighbors == null) {
												countNeighbors = 0;
											}
											levelTable.put(pnTemp,
													countNeighbors + 1);
											currentPath.add(pnTemp);
										}
//										System.out.println(current + " " + pn);
										if (!toVisit.contains(nodeToAdd)) {
											nextLevelToSee.add(nodeToAdd);
											lastLevelPath.put(nodeToAdd, pn);
										}
									}
								}
							}
						} // END FOR
						visited.add(current);
//						for(PathNeighbor temp : currentPath){
//							lastLevelPath.put(current, temp);
//						}
					} // END FOR LEVEL
					toVisit = nextLevelToSee;
					
					tables.addNodeLevelTable(levelTable, node, l);
				} // END FOR
				count++;
				// debug("Processed %d nodes", count);
				if (debugThreads && count % 1000 == 0) {
					debug("[T%d] Processed %d nodes", id, count);
				}
			} // END FOR
			if (debugThreads) {
				debug("[T%d] Processed %d nodes", id, count);
			}
			return tables;
		}
	}

	@Override
	public void compute() throws AlgorithmExecutionException {
		// DECLARATIONS
		ExecutorService nodePool = null;
		int chunkSize;
		List<Future<PathNeighborTables>> tableNodeFuture;
		Long[] graphNodes;
		PathNeighborTables tables;
		neighborTables = new PathNeighborTables(k);
		// END DECLARATIONS

		try {
			// Start a BFS on the whole graph
			if (nodeProcessed != null) {
				graphNodes = nodeProcessed.toArray(new Long[nodeProcessed
						.size()]);
			} else {
				graphNodes = graph.vertexSet().toArray(
						new Long[graph.vertexSet().size()]);
			}
//			debug("Computed the vertex set");
			if (graphNodes.length > numThreads * 2) {
				nodePool = Executors.newFixedThreadPool(numThreads);
				tableNodeFuture = new ArrayList<>();
				chunkSize = (int) Math.round(graphNodes.length / numThreads
						+ 0.5);
				for (int i = 0; i < numThreads; i++) {
					tableNodeFuture.add(nodePool
							.submit(new ComputePathNodeNeighbors(i + 1, graphNodes,
									i * chunkSize, (i + 1) * chunkSize)));

				}
//				System.out.println(tableNodeFuture.size());
				int m = 0;
				for (int i = 0; i < tableNodeFuture.size(); i++) {
					Future<PathNeighborTables> future = tableNodeFuture.get(i);
					tables = future.get();
					neighborTables.merge(tables);
//					System.out.println(m + "done");
					m++;
				}
			} else {
				neighborTables = new ComputePathNodeNeighbors(1, graphNodes, 0,
						graphNodes.length).call();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new AlgorithmExecutionException(
					"A generic error occurred, complete message follows", ex);
		} finally {
			if (nodePool != null) {
				nodePool.shutdown();
			}
		}
	}

	public void setK(int k) {
		this.k = k;
	}

	public void setGraph(Multigraph graph) {
		this.graph = graph;
	}

	public void setNumThreads(int numThreads) {
		this.numThreads = numThreads;
	}

	public PathNeighborTables getPathNeighborTables() {
		return neighborTables;
	}

	public void setLogThreads(boolean log) {
		this.debugThreads = log;
	}

	public void setNodeProcessed(Collection<Long> nodeProcessed) {
		this.nodeProcessed = nodeProcessed;
	}
}
