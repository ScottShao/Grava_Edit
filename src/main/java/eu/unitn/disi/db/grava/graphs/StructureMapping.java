package eu.unitn.disi.db.grava.graphs;

public class StructureMapping {
	private Edge gEdge;
	private Long qNode;

	public StructureMapping() {
		this.qNode = null;
		this.gEdge = null;
	}
	
	public StructureMapping(Long qNode, Edge gEdge){
		this.qNode = qNode;
		this.gEdge = gEdge;
	}
	
	public boolean equals(Object o){
		StructureMapping sm = (StructureMapping)o;
		if(this.qNode.equals(sm.getqNode()) && this.gEdge.equals(sm.getgEdge())){
			return true;
		}else{
			return false;
		}
	}
	
	

	public Long getqNode() {
		return qNode;
	}

	public void setqNode(Long qNode) {
		this.qNode = qNode;
	}

	public Edge getgEdge() {
		return gEdge;
	}

	public void setgEdge(Edge gEdge) {
		this.gEdge = gEdge;
	}

	
	
	

}
