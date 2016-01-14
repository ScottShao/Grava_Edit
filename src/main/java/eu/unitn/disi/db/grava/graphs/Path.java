package eu.unitn.disi.db.grava.graphs;

import java.util.ArrayList;

public class Path {
	private ArrayList<Long> path;
	public Path() {
		path = new ArrayList<Long>();
	}
	
	public Long getPreNode(){
		if(path.size() == 0){
			return null;
		}else{
			return path.get(path.size() -1);
		}
	}
	
	public boolean add(Long node){
		return path.add(node);
	}

	public ArrayList<Long> getPath() {
		return path;
	}

	public void setPath(ArrayList<Long> path) {
		this.path = path;
	}
	
}
