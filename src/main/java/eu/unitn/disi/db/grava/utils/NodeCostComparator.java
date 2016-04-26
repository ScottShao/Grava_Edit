package eu.unitn.disi.db.grava.utils;

import java.util.Comparator;

import eu.unitn.disi.db.grava.graphs.InfoNode;

public class NodeCostComparator implements Comparator<InfoNode> {

	@Override
	public int compare(InfoNode o1, InfoNode o2) {
		long l1 = o1.getBsCount()+o1.getCmpCount()+o1.getUptCount();
		long l2 = o2.getBsCount()+o2.getCmpCount()+o2.getUptCount();
		if(l1 > l2){
			return 1;
		}else{
			return -1;
		}
	}

	

}
