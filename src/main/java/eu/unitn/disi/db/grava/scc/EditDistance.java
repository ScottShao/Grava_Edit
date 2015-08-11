package eu.unitn.disi.db.grava.scc;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.exemplar.core.algorithms.ComputeGraphNeighbors;
import eu.unitn.disi.db.exemplar.core.algorithms.PruningAlgorithm;
import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.vectorization.NeighborTables;
import eu.unitn.disi.db.grava.graphs.MappedNode;
import eu.unitn.disi.db.mutilities.StopWatch;

public class EditDistance {

	public static void main(String[] args) throws ParseException, IOException, AlgorithmExecutionException{
		
		int repeat = 30;
		StopWatch watch = new StopWatch();
		watch.start();
		Multigraph G = new BigMultigraph("testGraph.txt","testGraph.txt", true);
		
		Multigraph Q = new BigMultigraph("query.txt","query.txt", true);
//		Q.vertexSet();	
		System.out.println("loading takes:" + watch.getElapsedTimeMillis() + " ms");
		Map<Long, Set<MappedNode>> queryGraphMapping = null;
		ComputeGraphNeighbors tableAlgorithm = new ComputeGraphNeighbors();
		NeighborTables queryTables;
		NeighborTables graphTables;
		int threshold = 0;
		int neighbourNum = 2;
		
		watch.reset();
		tableAlgorithm.setK(2);
		tableAlgorithm.setGraph(G);
		tableAlgorithm.setNumThreads(1);
		tableAlgorithm.compute();
		graphTables = tableAlgorithm.getNeighborTables();
//		Map<Long,Integer>[] gNodeTable = graphTables.getNodeMap(5648);
//		for(int i = 0; i < gNodeTable.length; i++){
//			Map<Long,Integer> nodeLevel = gNodeTable[i];
//			for(Entry<Long,Integer> temp : nodeLevel.entrySet()){
//				System.out.println(temp.getKey() + " " + temp.getValue());
//			}
//		}
//		Map<Long,Integer>[] gNodeTable = graphTables.getNodeMap(495248);
		
//		for(int i = 0; i < gNodeTable.length; i++){
//			Map<Long, Integer> gTable = gNodeTable[i];
//			System.out.println("Lvl:" + i);
//			for(Entry<Long, Integer> entry : gTable.entrySet()){
//				System.out.println(entry.getKey() + " " + entry.getValue());
//			}
//		}
		
//		System.out.println("Query table");
		tableAlgorithm.setGraph(Q);
		tableAlgorithm.compute();
		queryTables = tableAlgorithm.getNeighborTables();
		System.out.println("computing neighbor table time:" + watch.getElapsedTimeMillis());
//		Map<Long,Integer>[] qNodeTable = queryTables.getNodeMap(495248);
		
//		for(int i = 0; i < qNodeTable.length; i++){
//			Map<Long, Integer> qTable = qNodeTable[i];
//			System.out.println("Lvl:" + i);
//			for(Entry<Long, Integer> entry : qTable.entrySet()){
//				System.out.println(entry.getKey() + " " + entry.getValue());
//			}
//		}
		watch.reset();
		PruningAlgorithm pruningAlgorithm = new PruningAlgorithm();
		pruningAlgorithm.setGraph(G);
		pruningAlgorithm.setQuery(Q);
		pruningAlgorithm.setGraphTables(graphTables);
		pruningAlgorithm.setQueryTables(queryTables);
		pruningAlgorithm.setThreshold(threshold);
		pruningAlgorithm.compute();
		
		queryGraphMapping = pruningAlgorithm.getQueryGraphMapping();
		long pruningTime = System.nanoTime();
		System.out.println("pruning time:" + watch.getElapsedTimeMillis());

		watch.reset();
		Isomorphism iso = new Isomorphism();
		iso.setQueryEdges(Q.edgeSet());
		iso.setThreshold(threshold);
		iso.setQuery(Q);
		iso.setQueryGraphMapping(queryGraphMapping);
		iso.findIsomorphism();
		System.out.println("answer " + watch.getElapsedTimeMillis() + " ms");
	}

}
