package eu.unitn.disi.db.exemplar.core;

import eu.unitn.disi.db.grava.graphs.Multigraph;
import java.util.List;

public interface StartingNodeAlgorithm {

    public Long getStartingNode(Multigraph query);

    public List<Long> getStartingNodes(Multigraph query);
}
