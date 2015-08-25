package eu.unitn.disi.db.grava.scc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.exemplar.core.algorithms.ComputeGraphNeighbors;
import eu.unitn.disi.db.exemplar.core.algorithms.ComputePathGraphNeighbors;
import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.EdgeLabel;
import eu.unitn.disi.db.grava.graphs.MappedNode;
import eu.unitn.disi.db.grava.graphs.PathNeighbor;
import eu.unitn.disi.db.grava.vectorization.NeighborTables;
import eu.unitn.disi.db.grava.vectorization.PathNeighborTables;

public class Test {
	boolean test;
	int t;
	public static void main(String[] args) throws ParseException, IOException, AlgorithmExecutionException {
//		BigMultigraph G = new BigMultigraph("query.txt","query.txt",true);
//        ComputePathGraphNeighbors tableAlgorithm = new ComputePathGraphNeighbors();
//        tableAlgorithm.setK(2);
//		tableAlgorithm.setGraph(G);
//		tableAlgorithm.setNumThreads(1);
//		tableAlgorithm.compute();
//		PathNeighborTables table = tableAlgorithm.getPathNeighborTables();
		
//		for(Map.Entry<PathNeighbor, Integer> entry :table.getNodeMap(686848L)[1].entrySet()){
//			System.out.println(entry.getKey() + " " + entry.getValue());
//		}
		PathNeighbor pn = new PathNeighbor();
		pn.add(new EdgeLabel(0L, true));
		PathNeighbor a = new PathNeighbor();
		a.add(new EdgeLabel(1111,true));
		System.out.println(pn.equals(a));
		HashMap<PathNeighbor, Integer> test = new HashMap<>();
		test.put(pn, 1);
		test.put(a, 2);
		System.out.println(test.size());
    }
	
}
