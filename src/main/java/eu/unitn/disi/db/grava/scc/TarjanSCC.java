package eu.unitn.disi.db.grava.scc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Stack;

import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.LabelContainer;
import eu.unitn.disi.db.grava.utils.StdOut;

public class TarjanSCC {

//    private boolean[] marked;        // marked[v] = has v been visited?
//    private int[] id;                // id[v] = id of strong component containing v
//    private int[] low;               // low[v] = low number of v
    private int pre;                 // preorder number counter
    private int count;               // number of strongly-connected components
    private Stack<Long> stack;
    public static int numberOfNodes;
    private Collection<Long> vertexSet;
    private HashSet<Long> visited;
    private HashMap<Long, Integer> low;
    private int visitedNum;
//    private HashMap<Long, Integer> id;
//    private static HashMap<Integer, Long> IntToLong;
//    private static HashMap<Long, Integer> LongToInt;
    


    /**
     * Computes the strong components of the digraph <tt>G</tt>.
     * @param G the digraph
     */
    public TarjanSCC(BigMultigraph G) {
    	numberOfNodes = G.numberOfNodes();
    	vertexSet = G.vertexSet();
//    	try {
//			//IntToLong = this.constructIntToLong(vertexSet);
//		} catch (IOException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
    	//LongToInt = this.constructLongToInt(vertexSet);
//    	System.out.println("numb");
//        marked = new boolean[numberOfNodes];
        stack = new Stack<Long>();
        visited = new HashSet<Long>();
//        id = new HashMap<Long, Integer>(); 
        low = new HashMap<Long,Integer>();
        Iterator<Long> it = G.iterator();
        count = 0;
        int num = 0;
        visitedNum = 0;
        Long temp;
        while(it.hasNext()){
        	if(num % 5000 == 0){
        		System.out.println("DFS processing " + num);
        	}
        	temp = it.next();
        	if(!visited.contains(temp)){
        		try {
					dfs(G, temp);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        	}
        	num ++;
        }
//        for (int v = 0; v < numberOfNodes; v++) {
//        	if(v % 5000 == 0){
//        		System.out.println("DFS processing " + v);
//        	}
//            if (!marked[v])
//				try {
//					dfs(G, v);
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//        }

        // check that id[] gives strong components
        //assert check(G);
    }
    


	public HashMap<Integer, Long> constructIntToLong(Collection<Long> vertexSet) throws IOException{
    	HashMap<Integer, Long> mapping = new HashMap<Integer, Long>();
    	int i = 0;
    	File i2l = new File("Int2Long.txt");
    	BufferedWriter bw = new BufferedWriter(new FileWriter(i2l));
    	for(Long temp : vertexSet){
    		mapping.put(i, temp);
    		bw.write(temp.toString());
    		bw.newLine();
    		i++;
    	}
    	bw.close();
    	return mapping;
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
    public HashMap<Long, Integer> constructLongToInt(Collection<Long> vertexSet){
    	HashMap<Long, Integer> mapping = new HashMap<Long, Integer>();
    	int i = 0;
    	for(Long temp : vertexSet){
    		mapping.put(temp, i);
    	}
    	return mapping;
    }
    private void dfs(BigMultigraph G, Long v) throws IOException { 
//        marked[v] = true;
    	visitedNum ++;
    	if(visitedNum % 1000 == 0){
    		System.out.println("Visited nodes num: " + visitedNum);
    	}
    	visited.add(v);
//        low[v] = pre++;
    	low.put(v, pre++);
//        int min = low[v];
    	int min = pre;
        System.out.println("dfs: " + v);
        stack.push(v);
//        long temp = Int2Long(v);
//        System.out.println("long:" + temp);
        Collection<Long> adjNodes = G.adj(v);
        
        if(adjNodes != null){
        	System.out.println("adjNodes size:" + adjNodes.size());
	        for (Long w : adjNodes) {
//	        	int intW = Long2Int(w);
//	            if (!marked[intW]) dfs(G, intW);
//	            if (low[intW] < min) min = low[intW];
	        	if(!visited.contains(w)) dfs(G, w);
	        	int l = low.get(w);
	        	if(l < min) min = l;
	        }
        }else{
        	return;
        }
//        System.out.println("min, low[v]" + min + " " + low[v]);
        int lv = low.get(v);
//        if (min < low[v]) { low[v] = min; return; }
        if(min < lv) {
        	low.put(v, min);
        	return;
        }
        Long w;
        System.out.println("Writing " + count + " component");
        File out = new File("graph" + count + ".txt");
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out,true)));
        do {
        	w = stack.pop();
        	System.out.println("writing " + w);
        	
//            id[w] = count;
//            low[w] = numberOfNodes;
        	low.put(w, numberOfNodes);
          
            bw.write(w+"");
            bw.newLine();
        } while (w != v);
        bw.close();
        count++;
    }


    /**
     * Returns the number of strong components.
     * @return the number of strong components
     */
    public int count() {
        return count;
    }


    /**
     * Are vertices <tt>v</tt> and <tt>w</tt> in the same strong component?
     * @param v one vertex
     * @param w the other vertex
     * @return <tt>true</tt> if vertices <tt>v</tt> and <tt>w</tt> are in the same
     *     strong component, and <tt>false</tt> otherwise
     */
//    public boolean stronglyConnected(int v, int w) {
//        return id[v] == id[w];
//    }

    /**
     * Returns the component id of the strong component containing vertex <tt>v</tt>.
     * @param v the vertex
     * @return the component id of the strong component containing vertex <tt>v</tt>
     */
//    public int id(int v) {
//        return id[v];
//    }

    // does the id[] array contain the strongly connected components?
    private boolean check(BigMultigraph G) {
//        TransitiveClosure tc = new TransitiveClosure(G);
//        for (int v = 0; v < numberOfNodes; v++) {
//            for (int w = 0; w < numberOfNodes; w++) {
//                if (stronglyConnected(v, w) != (tc.reachable(v, w) && tc.reachable(w, v)))
//                    return false;
//            }
//        }
        return true;
    }
    
    public static int getNumOfNodes(){
    	return numberOfNodes;
    }

    
//    public static HashMap<Integer, Long> getIntToLong() {
//		return IntToLong;
//	}


//	public static HashMap<Long, Integer> getLongToInt() {
//		return LongToInt;
//	}


	/**
     * Unit tests the <tt>TarjanSCC</tt> data type.
	 * @throws IOException 
	 * @throws ParseException 
     */
    public static void main(String[] args) throws ParseException, IOException {
        BigMultigraph G = new BigMultigraph("graph10Nodes.txt","graph10Nodes.txt", true);
        HashMap<Long, LabelContainer> labelFreq = G.getLabelFreq();
        Collection<LabelContainer> lcs = labelFreq.values();
        if(lcs == null){
        	StdOut.println("null");
        }
        for(LabelContainer lc : lcs){
        	lc.print();
        }
//        TarjanSCC scc = new TarjanSCC(G);
//        long temp  = scc.Int2Long(1);
//        Collection<Edge> es = G.adjEdges(temp);
//        System.out.println("testing:");
//        for (Edge e : es){
//        	System.out.println(e.getSource() + " " + e.getDestination() + " " + e.getLabel());
//        }

    }

}


