package eu.unitn.disi.db.grava.graphs;

/**
 * @author Zhaoyang
 *
 */
public class InfoNode {
	private Long nodeID;
	private long bsCount;
	private int cmpCount;
	private int uptCount;
	private double sel;
	
	public InfoNode() {
		nodeID = null;
		bsCount = 0;
		cmpCount = 0;
		uptCount = 0;
		sel = 0;
	}
	
	public InfoNode(Long nodeID){
		this.nodeID = nodeID;
	}

	public Long getNodeID() {
		return nodeID;
	}

	public void setNodeID(Long nodeID) {
		this.nodeID = nodeID;
	}

	

	

	public long getBsCount() {
		return bsCount;
	}

	public void setBsCount(long bsCount) {
		this.bsCount = bsCount;
	}

	public int getCmpCount() {
		return cmpCount;
	}

	public void setCmpCount(int cmpCount) {
		this.cmpCount = cmpCount;
	}

	public int getUptCount() {
		return uptCount;
	}

	public void setUptCount(int uptCount) {
		this.uptCount = uptCount;
	}

	public double getSel() {
		return sel;
	}

	public void setSel(double sel) {
		this.sel = sel;
	}
	
	

}
