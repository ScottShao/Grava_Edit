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
import eu.unitn.disi.db.exemplar.core.EditDistanceQuery;
import eu.unitn.disi.db.exemplar.core.RelatedQuery;
import eu.unitn.disi.db.exemplar.core.algorithms.steps.EDMatchingRecursiveStep;
import eu.unitn.disi.db.grava.graphs.MappedNode;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.tool.ThreadPoolFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * This class contains a naive - RECURSIVE - implementation for Algorithm1 thus the solution obtained traversing the
 * graph in order to find subgraphs matching the pattern in the query given as input
 *
 * @author Matteo Lissandrini <ml at disi.unitn.eu>
 */
public class EditDistanceQuerySearch extends RelatedQuerySearch {

    public static long answerCount = 0;
    public static boolean isBad = false;
    public static long interNum = 0;
    private int threshold;
    private Long startingNode;
    private int searchCount;

    /**
     * Execute the algorithm
     */
    @Override
    public void compute() throws AlgorithmExecutionException {
//        Long startingNode = this.getRootNode(true);
        searchCount = 0;
        interNum = 0;
//    	debug("Starting node is %s",startingNode );
        if (startingNode == null) {
            throw new AlgorithmExecutionException("no root node has been found, and this is plain WR0NG!");
        }

        // FreebaseConstants.convertLongToMid(
        // debug("Root node %s ", startingNode);
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

        List<EditDistanceQuery> tmp = null;
//        System.out.println("threads num:" + this.getNumThreads());
        //Start in parallel

        ExecutorService pool = ThreadPoolFactory.getSearchThreadPool();
        int numThreads = ((ThreadPoolExecutor)pool).getMaximumPoolSize();

        int chunkSize = (int) Math.round(graphNodes.size() / numThreads + 0.5);
        List<Future<List<EditDistanceQuery>>> lists = new ArrayList<>();
        ////////////////////// USE 1 THREAD
        //chunkSize =  graphNodes.size();
        ////////////////////// USE 1 THREAD

        List<List<MappedNode>> nodesChunks = new LinkedList<>();
        List<MappedNode> tmpChunk = new LinkedList<>(); // NETBEANS!
        int count = 0, threadNum = 0;
        for (MappedNode node : graphNodes) {
            //if (nodesSimilarity(queryConcept, node) > MIN_SIMILARITY) {
            if (count % chunkSize == 0) {
                tmpChunk = new LinkedList<>();
                nodesChunks.add(tmpChunk);
            }
            tmpChunk.add(node);
            count++;
        }
        for (List<MappedNode> chunk : nodesChunks) {
            threadNum++;
            EDMatchingRecursiveStep graphI = new EDMatchingRecursiveStep(threadNum, chunk.iterator(), startingNode,
                    query, graph, true, this.getSkipSave(), threshold, this.getQueryToGraphMap(), chunkSize);
            lists.add(pool.submit(graphI));
        }

//        info("Number of Threads: %d/%d chunk size: %d Number of nodes %d", threadNum, this.getNumThreads(), chunkSize, graphNodes.size());
        //        if(graphNodes.size()==1){
        //            Long i = graphNodes.iterator().next();
        //            debug("The lucky node is %s ", i);
        //        }

        //Merge partial results
        try {
            for (Future<List<EditDistanceQuery>> list : lists) {
                tmp = list.get();
                if (tmp != null) {
                    //debug("Graph size: %d", smallGraph.vertexSet().size());
                    //                  //((List<RelatedQuery>)this.getRelatedQueries()).addAll(tmp);
                    this.getRelatedQueries().addAll(tmp);
                }
            }
        } catch (InterruptedException | ExecutionException ex) {
            ex.printStackTrace();
            error(ex.toString());
        }
        watch.stop();

    }

    public int getThreshold() {
        return threshold;
    }

    public void setThreshold(int threshold) {
        this.threshold = threshold;
    }

    public Long getStartingNode() {
        return startingNode;
    }

    public void setStartingNode(Long startingNode) {
        this.startingNode = startingNode;
    }


}
