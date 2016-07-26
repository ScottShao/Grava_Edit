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
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Stack;

import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.utils.FileOperator;

public class Sapper {
	public static Map<Long, Integer> nodeMap = new HashMap<>();
	public static Map<Long, Integer> nm = new HashMap<>();
	public static Map<Long, Integer> edgeMap = new HashMap<>();
	public static void write(String graph, String out) throws ParseException, IOException{
		BigMultigraph G = new BigMultigraph(graph + "nodes-sin.graph",
				graph + "nodes-sout.graph", false);
		BufferedWriter bw = new BufferedWriter(new FileWriter(out));
		bw.write("1");
		bw.newLine();
		bw.write("t # 0 " + G.vertexSet().size() + " " + G.edgeSet().size());
		bw.newLine();
		int vCount = 0;
		int eCount = 0;
		
		for (Long node : G.vertexSet()) {
			nodeMap.put(node, vCount);
			bw.write("v " + vCount + " " + vCount);
			bw.newLine();
			vCount++;
		}
		
		for (Edge e : G.edgeSet()) {
			int mappedID;
			if (edgeMap.containsKey(e.getLabel())) {
				mappedID= edgeMap.get(e.getLabel());
			} else {
				edgeMap.put(e.getLabel(), eCount);
				mappedID = eCount++;
			}
			bw.write("e " + nodeMap.get(e.getSource()) + " " + nodeMap.get(e.getDestination()) + " " + mappedID);
			bw.newLine();
		}
		bw.close();
	}
	
	public static void writeQuery(String graph, String out, BufferedWriter bw, int i) throws ParseException, IOException{
		BigMultigraph G = new BigMultigraph(graph,
				graph, true);
		bw.write("t # " + i +" " + G.vertexSet().size() + " " + G.edgeSet().size());
		bw.newLine();
		int vCount = 0;
		int eCount = 0;
		for (Long node : G.vertexSet()) {
			nm.put(node, vCount);
			bw.write("v " + vCount + " " + nodeMap.get(node));
			bw.newLine();
			vCount++;
		}
		
		for (Edge e : G.edgeSet()) {
			bw.write("e " + nm.get(e.getSource()) + " " + nm.get(e.getDestination()) + " " + edgeMap.get(e.getLabel()));
			bw.newLine();
		}
	}
	public static void main(String[] args) throws ParseException, IOException  {
		String graph = "o10000";
		String out = "software/sapper" + graph + ".txt";
		write(graph, out);
		ArrayList<String> queryFiles = FileOperator.getFileName("software/" + graph);
		BufferedWriter bw = new BufferedWriter(new FileWriter("software/s" + graph + "/q.txt"));
		System.out.println(queryFiles.size());
		bw.write(queryFiles.size() + "\n");
		int i = 0;
		for (String file : queryFiles) {
			String[] temp = file.split("/");
			System.out.println(i + " " + temp[temp.length - 1]);
			writeQuery(file, "software/s" + graph + "/" + temp[temp.length - 1], bw, i);
			i++;
		}
		bw.close();
	}

}
