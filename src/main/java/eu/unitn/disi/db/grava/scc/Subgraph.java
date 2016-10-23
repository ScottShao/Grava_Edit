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
	private Set<Long> visited;
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
		this.visited = new HashSet<>();
		visited.add(startingNode);
		while (visited.size() < maxNode) {
			this.readFile();
		}
//		bfs();
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
			BufferedWriter bw = new BufferedWriter(new FileWriter(output));
			String line = null;
			int count = 0;
			while ((line = bf.readLine()) != null) {
				String[] words = line.split(" ");
				Long src = Long.parseLong(words[0]);
				Long des = Long.parseLong(words[1]);
				Long label = Long.parseLong(words[2]);
				if (visited.contains(src) || visited.contains(des)) {
					visited.add(src);
					visited.add(des);
					bw.write(src + " " + des + " " + label);
					bw.newLine();
					if (visited.size() >= maxNode) break;
				}
				
			}
			System.out.println("crt nodes number " + visited.size());
			bw.flush();
			bf.close();
			bw.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws ParseException, IOException {
		Subgraph s = new Subgraph(1000000, 48L, "freebase-sin.graph", "1M.graph");
	}

}
