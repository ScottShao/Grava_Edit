package eu.unitn.disi.db.grava.graphs;

public class LabelPath {
	private long firstLabel;
	private long secLabel;
	
	public LabelPath() {
		firstLabel = -1L;
		secLabel = -2L;
	}
	
	public LabelPath(long firstLabel, long secLabel){
		this.firstLabel = firstLabel;
		this.secLabel = secLabel;
	}
	
	public String toString(){
		return firstLabel + "->" + secLabel;
	}
	
	public int hashCode(){
		return (int)firstLabel%10000;
	}
	
	public boolean equals(Object o){
		LabelPath lp = (LabelPath)o;
		if(this.firstLabel == lp.getFirstLabel() && this.secLabel == lp.getSecLabel()){
			return true;
		}else{
			return false;
		}
	}

	public long getFirstLabel() {
		return firstLabel;
	}

	public void setFirstLabel(long firstLabel) {
		this.firstLabel = firstLabel;
	}

	public long getSecLabel() {
		return secLabel;
	}

	public void setSecLabel(long secLabel) {
		this.secLabel = secLabel;
	}
	
	
}
