package eu.unitn.disi.db.grava.graphs;

public class GraphRing {
	private Long firstNode;
	private Long secondNode;
	
	public GraphRing() {
		this.firstNode = null;
		this.secondNode = null;
	}
	
	public GraphRing(Long firstNode, Long secondNode) {
		this.firstNode = firstNode;
		this.secondNode = secondNode;
	}

	public boolean equals(Object o){
		boolean flag = false;
		GraphRing gr = (GraphRing)o;
		if(this.firstNode.equals(gr.getFirstNode()) && this.secondNode.equals(gr.getSecondNode())){
			flag = true;
		}
		if(this.firstNode.equals(gr.getSecondNode()) && this.secondNode.equals(gr.getFirstNode())){
			flag = true;
		}
		return flag; 
	}

	public int hashCode(){
		return (int) (this.firstNode + this.secondNode);
	}
	
	public Long getFirstNode() {
		return firstNode;
	}

	public void setFirstNode(Long firstNode) {
		this.firstNode = firstNode;
	}

	public Long getSecondNode() {
		return secondNode;
	}

	public void setSecondNode(Long secondNode) {
		this.secondNode = secondNode;
	}
	
	
}
