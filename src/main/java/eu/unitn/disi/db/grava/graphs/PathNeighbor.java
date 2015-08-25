package eu.unitn.disi.db.grava.graphs;

import java.util.ArrayList;

public class PathNeighbor {
	private ArrayList<EdgeLabel> path;
	

	public PathNeighbor() {
		this.path = new ArrayList<EdgeLabel>();
	}
	
	public PathNeighbor(ArrayList<EdgeLabel> path){
		this.path = path;
	}
	
	public PathNeighbor(PathNeighbor pn){
		ArrayList<EdgeLabel> newPath = pn.getPath();
		path = new ArrayList<EdgeLabel>(newPath);
	}
	
	public boolean add(EdgeLabel newLabel){
		return path.add(newLabel);
	}
	
	public int hashCode(){
		return path.size();
	}

	public boolean equals(Object o){
		PathNeighbor np = (PathNeighbor)o;
		ArrayList<EdgeLabel> newPath = np.getPath();
		if(this.path.size() == newPath.size()){
			for(int i = 0; i < this.path.size(); i++){
				if(path.get(i).getLabel() == 0 || newPath.get(i).getLabel() == 0){
					continue;
				}
				if(!path.get(i).equals(newPath.get(i))){
					return false;
				}
			}
		}
		return true;
	}

	public ArrayList<EdgeLabel> getPath() {
		return path;
	}

	public void setPath(ArrayList<EdgeLabel> path) {
		this.path = path;
	}
	
	public int wildCardsNum(){
		int count = 0;
		for(EdgeLabel el : path){
			if(el.getLabel() == 0){
				count ++;
			}
		}
		return count;
	}
	public String toString(){
		StringBuilder sb = new StringBuilder();
		for(EdgeLabel el : path){
			sb.append(el);
		}
		return sb.toString();
	}
	
}
