/**
 * 
 */
package eu.unitn.disi.db.grava.scc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.utils.FileOperator;

/**
 * @author Zhaoyang
 *
 */
public class GenerateError {
	private Random rn;
	private Edge[] edgeSet;
	/**
	 * 
	 */
	public GenerateError(BigMultigraph G, String folder) {
		rn = new Random();
		edgeSet = G.edgeSet().toArray(new Edge[0]);
		File error = new File(folder + "error/");
		if (!error.exists()) {
			error.mkdir();
		}
		ArrayList<String> queryFiles = FileOperator.getFileName(folder);
		for (String queryFile : queryFiles) {
//			System.out.println(queryFile);
			if (queryFile.contains("csv")) continue;
			try {
				List<Edge> edges = getEdges(queryFile);
				generateError(edges);
				write(edges, queryFile);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private void write(List<Edge> edges, String queryFile) throws IOException {
		String[] des = queryFile.split("/");
		String folder = queryFile.substring(0, queryFile.length() - des[des.length - 1].length()) + "error/";
		String fileName = queryFile.substring(queryFile.length() - des[des.length - 1].length());
		BufferedWriter bw = new BufferedWriter(new FileWriter(folder+fileName));
		for (Edge e : edges) {
			bw.write(e.getSource() + " " + e.getDestination() + " " + e.getLabel());
			bw.newLine();
		}
		bw.close();
	}
	
	private void generateError(List<Edge> edges) {
		int idx = rn.nextInt(edges.size());
		System.out.println(edges.size() + " " + idx);
		Edge e = edges.remove(idx);
		edges.add(new Edge(e.getSource(), e.getDestination(), edgeSet[rn.nextInt(edgeSet.length)].getLabel()));
	}
	
	private List<Edge> getEdges(String fileName) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		List<Edge> edges = new ArrayList<>();
		String line = null;
		while ((line = br.readLine()) != null) {
			String[] words = line.split(" ");
			edges.add(new Edge(Long.parseLong(words[0]), Long.parseLong(words[1]), Long.parseLong(words[2])));
		}
		return edges;
	}
	
	public static void main(String[] args) throws ParseException, IOException {
		String graph = "500000nodes";
		BigMultigraph G = new BigMultigraph(graph + "-sin.graph",
				graph + "-sout.graph", false);
		GenerateError s = new GenerateError(G, "queryFolder/" + graph + "/");
	}

}
