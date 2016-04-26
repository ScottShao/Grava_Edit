package eu.unitn.disi.db.grava.utils;

import java.util.Comparator;

import eu.unitn.disi.db.grava.graphs.InfoNode;

public class NodeBSComparator implements Comparator<InfoNode>{

	@Override
	public int compare(InfoNode o1, InfoNode o2) {
		if(o1.getBsCount() > o2.getBsCount()){
			return 1;
		}else{
			return -1;
		}
	}
}

