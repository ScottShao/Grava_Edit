package eu.unitn.disi.db.grava.graphs;

public class ParentChild {
	private Long parent;
	private Long child;
	
	public ParentChild() {
		parent = null;
		child = null;
	}
	
	public ParentChild(Long parent, Long child){
		this.parent = parent;
		this.child = child;
	}
	
	public int hashCode(){
		return (int)(parent + child)%100000;
	}
	
	public boolean equals(Object o){
		ParentChild pc = (ParentChild)o;
		if(pc.getParent().equals(this.parent) && pc.getChild().equals(this.child)){
			return true;
		}else{
			return false;
		}
	}

	public Long getParent() {
		return parent;
	}

	public void setParent(Long parent) {
		this.parent = parent;
	}

	public Long getChild() {
		return child;
	}

	public void setChild(Long child) {
		this.child = child;
	}
	
	public String toString(){
		return "parent:" + this.parent + " child:" + this.child; 
	}

}
