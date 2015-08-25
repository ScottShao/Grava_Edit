package eu.unitn.disi.db.grava.graphs;

public class EdgeLabel {
	private long label;
	private boolean isIncoming;
	
	public EdgeLabel() {
		this.label = -1;
		this.isIncoming = false;
	}
	
	public EdgeLabel(long label, boolean isIncoming){
		this.label = label;
		this.isIncoming = isIncoming;
	}

	public long getLabel() {
		return label;
	}

	public void setLabel(long label) {
		this.label = label;
	}

	public boolean isIncoming() {
		return isIncoming;
	}

	public void setIncoming(boolean isIncoming) {
		this.isIncoming = isIncoming;
	}
	
	public boolean equals(Object o){
		EdgeLabel el = (EdgeLabel)o;
		if(el.getLabel() == label && el.isIncoming() == this.isIncoming){
			return true;
		}
		return false;
	}
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append(this.label);
		if(isIncoming){
			sb.append("<--");
		}else{
			sb.append("-->");
		}
		return sb.toString();
	}
	
}
