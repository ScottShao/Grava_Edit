package eu.unitn.disi.db.grava.graphs;

public class Vertex {
	private boolean marked;
	private int componentID;
	private int low;
	
	public Vertex(){
		marked = false;
		componentID = -1;
		low = -1;
		
		
	}

}
