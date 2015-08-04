package eu.unitn.disi.db.grava.scc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashSet;

import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.MappedNode;

public class Test {
	boolean test;
	int t;
	public static void main(String[] args) throws ParseException, IOException {
//        BigMultigraph G = new BigMultigraph("1st.txt","1st.txt");
//        long[][] inEdges = G.getInEdges();
//        long[][] outEdges = G.getOutEdges();
//        System.out.println("inEdges:");
//        for(int i =0; i < inEdges.length; i++){	
//        	System.out.println(inEdges[i][0] + " "+inEdges[i][1] + " "+inEdges[i][2]);
//        }
//        System.out.println("outEdges:");
//        for(int i =0; i < outEdges.length; i++){
//        	System.out.println(outEdges[i][0] + " "+outEdges[i][1] + " "+outEdges[i][2]);
//        }
		Test t = new Test();
		System.out.println(t.t);
    }
	
}
