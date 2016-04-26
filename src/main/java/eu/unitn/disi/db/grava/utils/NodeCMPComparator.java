package eu.unitn.disi.db.grava.utils;

import java.util.Comparator;

import eu.unitn.disi.db.grava.graphs.InfoNode;


public class NodeCMPComparator implements Comparator<InfoNode> {

	@Override
	public int compare(InfoNode o1, InfoNode o2) {
		return o1.getCmpCount() - o2.getCmpCount();
	}

}
