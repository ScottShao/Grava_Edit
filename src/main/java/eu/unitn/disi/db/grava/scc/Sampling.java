package eu.unitn.disi.db.grava.scc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
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
//	private boolean[] marked;
	private Collection<Long> vertexSet;
//    private static HashMap<Integer, Long> IntToLong;
//    private static HashMap<Long, Integer> LongToInt;
    private int visitedNodesNum;
    private int numberOfNodes;
    private int count;
    private Queue<Integer> distance;
    private HashSet<Long> visited;
    private int dis;
    private int pre;
    private double rate;
    private int ansNum;
    private int ans;
    
	public Sampling(BigMultigraph G){
		this.G = G;
	}
	public Sampling(BigMultigraph G,int maxNodesNum, int maxDegree, Long startingNode, String fileName) throws IOException{
		this.G = G;
		numberOfNodes = G.numberOfNodes();
		visitedNodesNum = 0;
		vertexSet = G.vertexSet();
//    	IntToLong = this.constructIntToLong(vertexSet);
//    	LongToInt = this.constructLongToInt(vertexSet);
		queue = new LinkedList<Long>();
		distance =new LinkedList<Integer>();
		visited = new HashSet<Long>();
//		marked = new boolean[G.numberOfNodes()];
		this.maxDegree = maxDegree;
		this.maxNodesNum = maxNodesNum;
		this.startingNode = startingNode;
		ansNum = 2;
		ans = 0;
		count = 0;
		dis = 0;
		pre = -1;
		distance.add(dis);
		rate =  0.1;
		this.bfs(G, startingNode,fileName);
	}
    public Long Int2Long(int in){
    	int i = 0;
    	for(Long temp : vertexSet){
    		if(i == in){
    			return temp;
    		}
    		i++;
    	}
    	return (long) -1;
    	
    }
    public int Long2Int(Long l){
    	int i = 0;
    	for(Long temp : vertexSet){
    		if(temp.equals(l)){
    			return i;
    		}
    		i++;
    	}
    	return -1;
    	
    }
    
    
	private void bfs(BigMultigraph G, Long v, String fileName) throws IOException{
//		marked[v] = true;
//        System.out.println("bfs: " + v);
//        Long test = queue.poll();
//        
//        if(test.equals(v)){
//        	System.out.println("Succeed");
//        }else{
//        	System.out.println("Fail");
//        }
//        long temp = Int2Long(v);
//        System.out.println("long:" + temp);
		File out = new File(fileName);
        BufferedWriter bw = new BufferedWriter(new FileWriter(out));
		queue.add(v);
		count++;
        Collection<Edge> adjEdges = null;
        Long top;
        Long newNode;
        Long prevNode;
        int degree;
        int cur;
        boolean flag = true;
        while(flag && !queue.isEmpty()){
        	top = queue.poll();
        	cur = distance.poll();
        	
        	if(visited.contains(top)){
        		continue;
        	}
        	visited.add(top);
        	System.out.println("current visiting:" + top);
        	adjEdges = G.adjEdges(top);
//        	System.out.println(adjEdges.size());
        	degree = 0;
        	if(cur != pre){
        		dis++;
        		pre = cur;
        		rate *= 1/Math.exp(dis);
//        		System.out.println("distance " + cur +" rate " + rate);
        	}
        	for(Edge e : adjEdges){
//        		System.out.println("Count:" + count);
//        		System.out.println("degree:" + degree);
        		if(count > this.maxNodesNum-1){
        			flag = false;
        			System.out.println("maximum nodes num");
        			break;
        		}
        		if(degree > this.maxDegree-1){
        			System.out.println("maximum degree exit");
        			break;
        		}
        		newNode = (e.getSource().equals(top))?e.getDestination():e.getSource();
//        		System.out.println("new Nodes" + newNode);
        		if(!visited.contains(newNode)){
	        		queue.add(newNode);
	        		distance.add(dis);
//	        		System.out.println(e.getSource() + " " + e.getDestination() + " " + e.getLabel());
//	        		bw.write(e.getSource() + " " + e.getDestination() + " " + e.getLabel());
	        		System.out.println(newNode);
	        		System.out.println(e.getSource() + " " + e.getDestination() + " " + e.getLabel());
	        		bw.write(e.getSource() + " " + e.getDestination() + " " + e.getLabel());
	        		bw.newLine();
	        		
//	        		if(ans <= ansNum && Math.random() <= rate){
//	        			
//	        			ans++;
//	        			bw.write(e.getDestination() + " " + "5378845548 1000014827");
//	        			bw.newLine();
//	        			bw.write("5378845548 828277504948 1000008783");
//	        			bw.newLine();
//	        			bw.write("828277504948 82878277755348 1000008725");
//	        			bw.newLine();
//	        			bw.write("82878277755348 5378817448 1000008783");
//	        			bw.newLine();
//	        			System.out.println("current answer number:" + ans + "/" + ansNum);
////	        			System.out.println("printing " + rate + " " + dis);
//	        		}
	        		count++;
	        		degree++; // out degree
        		}
        		
        	}
        	
//        	System.out.println("rate:" + rate);
        	
        }
        bw.close();
       
//        if(count > this.maxNodesNum){
//        	System.out.println("Exceed the maximum nodes number");
//        	return;
//        }
//        if(adjNodes != null){
//        	System.out.println("adjNodes size:" + adjNodes.size());
//        	int degree = 0;
//	        for (Edge e : adjNodes) {
//	        	Long w = (e.getSource() == v)?e.getSource():e.getDestination();
//	        	degree++;
//	        	if(degree > this.maxDegree){
//	        		System.out.println("Exceed the maximum degree");
//	        		break;
//	        	}
//	        	int intW = Long2Int(w);
//	            if (!marked[intW]) {
//	            	System.out.println(e.getSource() + " " + e.getDestination() + " " + e.getLabel());
//	            	queue.add(intW);
//	            	bfs(G, intW);
//	            }
//	        }
//        }else{
//        	return;
//        }
	}
	
    public HashMap<Integer, Long> constructIntToLong(Collection<Long> vertexSet){
    	HashMap<Integer, Long> mapping = new HashMap<Integer, Long>();
    	int i = 0;
    	for(Long temp : vertexSet){
    		mapping.put(i, temp);
    	}
    	return mapping;
    }
    
    public HashMap<Long, Integer> constructLongToInt(Collection<Long> vertexSet){
    	HashMap<Long, Integer> mapping = new HashMap<Long, Integer>();
    	int i = 0;
    	for(Long temp : vertexSet){
    		mapping.put(temp, i);
    	}
    	return mapping;
    }
    
    private void generateQuery(int edgeNum){
    	int count = 0;
    	Collection<Long> nodes = G.vertexSet();
    	Collection<Edge> edges;
    	int j;
    	long currentNode;
    	edgeNum --;
    	for(Long node : nodes){
    		edges = G.adjEdges(node);
    		currentNode = node;
    		for(Edge e : edges){
    			List<ArrayList<Edge>> results = new ArrayList<ArrayList<Edge>>();
    			List<Edge> currentEdges = new ArrayList<Edge>();
    			currentEdges.add(e);
    			
    			currentNode = ((node.equals(e.getDestination()))? e.getSource():e.getDestination());
    			this.dfs(results, edges, currentEdges, currentNode, edgeNum);
    			for(ArrayList<Edge> result : results){
    				System.out.println(node);
    				System.out.println("path query " + count);
    				for(Edge r : result){
    					System.out.println(r);
    				}
    				System.out.println("fan query:" + count);
    				for(Edge r : result){
    					System.out.println(this.getEdgeWithLabel(edges, r.getLabel()));
    				}
    				count ++;
    			}
    		}
    	}
    	
    }
    
    private void dfs(List<ArrayList<Edge>> results, Collection<Edge> adjEdges, List<Edge> currentEdges, Long currentNode, int edgeNum){
    	
    	if(edgeNum == 0){
    		ArrayList<Edge> result = new ArrayList<Edge>();
    		for(int i =0; i< currentEdges.size(); i++){
    			result.add(currentEdges.get(i));
    		}
    		results.add(result);
    	}else{
    		edgeNum --;
    		Collection<Edge> currentAdj = G.adjEdges(currentNode);
    		boolean nextRecur;
    		for(Edge e : currentAdj){
    			if(e.equals(currentEdges.get(currentEdges.size()-1))){
    				System.out.println("nonono");
    				continue;
    			}
    			nextRecur = false;
    			if(this.isContainLabel(adjEdges, e.getLabel())){
    				if(!this.isContainLabel(currentEdges, e.getLabel())){
    					currentEdges.add(e);
    					nextRecur = true;
    				}else{
    					int count = 0;
    					for(int j = 0; j < currentEdges.size(); j++){
    						if(currentEdges.get(j).getLabel().equals(e.getLabel())){
    							count ++;
    						}
    					}
    					for(Edge adjE : adjEdges){
    						if(adjE.getLabel().equals(e.getLabel())){
    							count --;
    							if(count <= 0){
    	    						currentEdges.add(e);
    	    						nextRecur = true;
    	    						break;
    	    					}
    						}
    					}
    					
    				}
    				if(nextRecur){
    					currentNode = (currentNode == e.getDestination())?e.getSource():e.getDestination();
    					dfs(results,adjEdges, currentEdges,currentNode, edgeNum);
    					currentEdges.remove(currentEdges.size()-1);
    				}
    				
    			}else{
    				continue;
    			}
    			
    		}
    	}
    }
    
    private boolean isContainLabel(Collection<Edge> adjEdges, Long label){
    	for(Edge e : adjEdges){
    		if(e.getLabel().equals(label)){
    			return true;
    		}
    	}
    	return false;
    }
    
    private Edge getEdgeWithLabel(Collection<Edge> adjEdges, Long label){
    	for(Edge e : adjEdges){
    		if(e.getLabel().equals(label)){
    			return e;
    		}
    	}
    	return null;
    }
    

    
    public static void main(String[] args) throws ParseException, IOException {
        BigMultigraph G = new BigMultigraph("10000nodes-sin.graph","10000nodes-sout.graph", false);
//        int size = G.vertexSet().size();
//        Random rnd = new Random();
//        Long[] nodes = G.vertexSet().toArray(new Long[size]);
//        for(int i = 0; i < 10; i++){
//        	System.out.println("Starting node " + nodes[rnd.nextInt(size)]);
//        	for(int j = 2; j <=5; j++){
//        		for(int k = 1;k <= 5; k++){
//        			Sampling s = new Sampling(G, j*10, k,  nodes[rnd.nextInt(size)], "Q" + j*10 + "N"+k+"D_" + (i+1) +".txt");
//        		}
//        	}
//        }
        Sampling s = new Sampling(G);
        s.generateQuery(2);
    }
}
