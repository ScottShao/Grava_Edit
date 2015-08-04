package eu.unitn.disi.db.grava.graphs;

public class MappedNode {
	private int dist;
	private long nodeID;
	private Edge mappedEdge;
	private boolean incoming;
	private Long prevQueryNode;
	private boolean isLabelDif;
	
	
	public MappedNode() {
		this.mappedEdge = null;
		this.dist = 0;
		this.nodeID = -1;
		this.incoming = false;
		this.isLabelDif = false;
	}
	
	public MappedNode(long nodeID, Edge mappedEdge, int dist, boolean incoming, boolean isLabelDif){
		this.dist = dist;
		this.nodeID = nodeID;
		this.mappedEdge = mappedEdge;
		this.incoming = incoming;
		this.isLabelDif = isLabelDif;
	}


	public Edge getMappedEdge() {
		return mappedEdge;
	}

	public void setMappedEdge(Edge mappedEdge) {
		this.mappedEdge = mappedEdge;
	}

	public boolean isIncoming() {
		return incoming;
	}

	public void setIncoming(boolean incoming) {
		this.incoming = incoming;
	}

	public int getDist() {
		return dist;
	}

	public void setDist(int dist) {
		this.dist = dist;
	}

	public long getNodeID() {
		return nodeID;
	}

	public void setNodeID(long nodeID) {
		this.nodeID = nodeID;
	}
	
	public boolean equals(Object o){
		MappedNode mn = (MappedNode)o;
		if(mn.mappedEdge.equals(this.mappedEdge) && mn.isIncoming() == this.incoming){
			return true;
		}else{
			return false;
		}
	}
	
	public int hashCode(){
		return (int) this.nodeID;
	}

	public String toString(){
		if(mappedEdge == null){
			return this.nodeID + " " + "null" + " " + this.dist;
		}else{
			return this.nodeID + " " + this.mappedEdge.toString() + " " + this.dist;
		}
	}

	public Long getPrevQueryNode() {
		return prevQueryNode;
	}

	public void setPrevQueryNode(Long prevQueryNode) {
		this.prevQueryNode = prevQueryNode;
	}

	public boolean isLabelDif() {
		return isLabelDif;
	}

	public void setLabelDif(boolean isLabelDif) {
		this.isLabelDif = isLabelDif;
	}
	
}
