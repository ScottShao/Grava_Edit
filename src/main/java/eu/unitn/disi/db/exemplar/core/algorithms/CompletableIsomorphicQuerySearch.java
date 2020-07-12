/*
 * Copyright (C) 2012 Matteo Lissandrini <ml at disi.unitn.eu>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package eu.unitn.disi.db.exemplar.core.algorithms;

import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.command.util.StopWatch;
import eu.unitn.disi.db.exemplar.core.RelatedQuery;
import eu.unitn.disi.db.exemplar.core.algorithms.steps.GraphIsomorphismRecursiveStep;
import eu.unitn.disi.db.grava.graphs.LabelContainer;
import eu.unitn.disi.db.grava.graphs.MappedNode;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * This class contains a naive - RECURSIVE - implementation for Algorithm1 thus the solution obtained traversing the
 * graph in order to find subgraphs matching the pattern in the query given as input
 *
 * @author Matteo Lissandrini <ml at disi.unitn.eu>
 */
public class CompletableIsomorphicQuerySearch extends RelatedQuerySearch {

    public static long answerCount = 0;
    public static boolean isBad = false;
    public static long interNum = 0;
    private Long startingNode;//TODO: set the best starting node
    private HashMap<Long, LabelContainer> labelFreq;
    private ExecutorService pool;
    private List<Callable<List<RelatedQuery>>> callables;

    /**
     * Execute the algorithm
     */
    @Override
    public void compute() throws AlgorithmExecutionException {
        if (startingNode == null) {
            throw new AlgorithmExecutionException("no root node has been found, and this is plain WR0NG!");
        }
        interNum = 0;
        if (this.getGraph().edgeSet().isEmpty()) {
            throw new AlgorithmExecutionException("NO KB Edges to find a root node!");
        }

        if (this.getQuery().edgeSet().isEmpty()) {
            throw new AlgorithmExecutionException("NO Query Edges to find a root node!");
        }
        this.setRelatedQueries(new HashSet<>());

        Multigraph graph = this.getGraph();
        Multigraph query = this.getQuery();

        Collection<MappedNode> graphNodes;

        StopWatch watch = new StopWatch();
        watch.start();
        graphNodes = null;
        if (this.getQueryToGraphMap() == null) {
            graphNodes = graph.infoVertexSet();
        } else {
            graphNodes = ((Map<Long, Set<MappedNode>>) this.getQueryToGraphMap()).get(startingNode);
        }

        int chunkSize = (int) Math.round(graphNodes.size() / getNumThreads() + 0.5);
        callables = new ArrayList<>();

        List<List<MappedNode>> nodesChunks = new LinkedList<>();
        List<MappedNode> tmpChunk = new LinkedList<>(); // NETBEANS!
        int count = 0, threadNum = 0;
        for (MappedNode node : graphNodes) {
            if (count % chunkSize == 0) {
                tmpChunk = new LinkedList<>();
                nodesChunks.add(tmpChunk);
            }

            tmpChunk.add(node);
            count++;
        }

        for (List<MappedNode> chunk : nodesChunks) {
            threadNum++;
            Callable<List<RelatedQuery>> graphI = new GraphIsomorphismRecursiveStep(threadNum, chunk.iterator(),
                    startingNode, query, graph, true, this.getSkipSave(), chunk.size(), this.getQueryToGraphMap(), labelFreq);
            callables.add(graphI);
        }
    }

    public List<Callable<List<RelatedQuery>>> getCallables() {
        return callables;
    }

    public Long getStartingNode() {
        return startingNode;
    }

    public void setStartingNode(Long startingNode) {
        this.startingNode = startingNode;
    }

    public HashMap<Long, LabelContainer> getLabelFreq() {
        return labelFreq;
    }

    public void setLabelFreq(HashMap<Long, LabelContainer> labelFreq) {
        this.labelFreq = labelFreq;
    }
}
