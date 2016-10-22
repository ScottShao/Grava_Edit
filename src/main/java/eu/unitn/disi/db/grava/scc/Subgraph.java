/**
 * 
 */
package eu.unitn.disi.db.grava.scc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;

/**
 * @author Zhaoyang
 *
 */
public class Subgraph {
	class Tuple {
		Long src;
		Long des;
		Long label;
		public Tuple(Long src, Long des, Long label) {
			this.src = src;
			this.des = des;
			this.label = label;
		}
	}
	private Map<Long, Set<Tuple>> in;
	private Map<Long, Set<Tuple>> out;
	private int maxNode;
	private Long startingNode;
	private String input;
	private String output;
	/**
	 * @throws IOException 
	 * 
	 */
	public Subgraph(int maxNode,  Long startingNode, String input, String output) throws IOException {
		this.maxNode = maxNode;
		this.startingNode = startingNode;
		this.input = input;
		this.output = output;
		this.in = new HashMap<>();
		this.out = new HashMap<>();
		this.readFile();
		bfs();
	}
	
	private void bfs() throws IOException {
		Set<Long> visited = new HashSet<>();
		Queue<Long> queue = new LinkedList<>();
		queue.add(startingNode);
		visited.add(startingNode);
		int count = 0;
		BufferedWriter bw = new BufferedWriter(new FileWriter(output));
		while (!queue.isEmpty()) {
			Long crt = queue.poll();
			visited.add(crt);
			Set<Tuple> adj = in.get(crt);
			System.out.println("crt" + crt);
			if (adj != null) {
				for (Tuple t : adj) {
					Long next = t.src;
					
					if (!visited.contains(next)) {
						queue.add(next);
						bw.write(t.src + " " + t.des + " " + t.label);
						bw.newLine();
					}
				}
			}
			adj = out.get(crt);
			if (adj != null) {
				for (Tuple t : adj) {
					Long next = t.des;
					System.out.println(next);
					if (!visited.contains(next)) {
						queue.add(next);
						bw.write(t.src + " " + t.des + " " + t.label);
						bw.newLine();
					}
				}
			}
			count++;
			if (count > maxNode) {
				 break;
			}
		}
		bw.flush();
		bw.close();
		
	}
	private void readFile() {
		try {
			BufferedReader bf = new BufferedReader(new FileReader(input));
			String line = null;
			int count = 0;
			while ((line = bf.readLine()) != null) {
				String[] words = line.split(" ");
				Long src = Long.parseLong(words[0]);
				Long des = Long.parseLong(words[1]);
				Long label = Long.parseLong(words[2]);
				Tuple one = new Tuple(src, des, label);
				Set<Tuple> set = in.get(des);
				if (set == null) {
					set = new HashSet<>();
					in.put(des, set);
				}
				set.add(one);
				set = out.get(src);
				if (set == null) {
					set = new HashSet<>();
					out.put(src, set);
				}
				set.add(one);
				count++;
				if (count % 100000 == 0) System.out.println("Processing " + count + "  lines");
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws ParseException, IOException {
		Subgraph s = new Subgraph(1000000, 70578472568248L, "freebase-sin.graph", "1M.graph");
	}

}
