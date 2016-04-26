package eu.unitn.disi.db.grava.utils;

import java.util.Comparator;

import eu.unitn.disi.db.grava.graphs.InfoNode;

public class NodeUPTComparator implements Comparator<InfoNode> {


	@Override
	public int compare(InfoNode o1, InfoNode o2) {
		return o1.getUptCount() - o2.getUptCount();
	}

}
