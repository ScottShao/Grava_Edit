package eu.unitn.disi.db.exemplar.core;

import eu.unitn.disi.db.grava.graphs.Multigraph;

public interface StartingNodeAlgorithm {

    public Long getStartingNode(Multigraph query);
}
