package eu.unitn.disi.db.grava.graphs;

public class MappedEdge {
	private Edge edge;
	private int dis;

	public MappedEdge() {
		edge = null;
		dis = 0;
	}
	
	public MappedEdge(Edge edge, int dis){
		this.edge = edge;
		this.dis = dis;
	}
	
	public boolean equals(Edge e){
		if(edge.getLabel() == e.getLabel()){
			return true;
		}else{
			return false;
		}
	}
	
	public Edge getEdge() {
		return edge;
	}

	public void setEdge(Edge edge) {
		this.edge = edge;
	}

	public int getDis() {
		return dis;
	}

	public void setDis(int dis) {
		this.dis = dis;
	}
	
	public String toString(){
		return this.edge.toString() + " dis:" + this.dis;
	}

}
