package eu.unitn.disi.db.grava.utils;

import java.util.Comparator;

import eu.unitn.disi.db.grava.graphs.InfoNode;

public class NodeSelComparator implements Comparator<InfoNode> {


	@Override
	public int compare(InfoNode o1, InfoNode o2) {
		if(o1.getSel() > o2.getSel()){
			return 1;
		}else{
			return -1;
		}
	}

}
