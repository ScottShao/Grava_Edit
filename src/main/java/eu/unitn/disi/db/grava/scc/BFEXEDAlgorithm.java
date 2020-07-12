package eu.unitn.disi.db.grava.scc;

import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.exemplar.core.RelatedQuery;
import eu.unitn.disi.db.exemplar.core.StartingNodeAlgorithm;
import eu.unitn.disi.db.exemplar.core.StartingNodeBaseAlgorithm;
import eu.unitn.disi.db.exemplar.core.algorithms.ComputeGraphNeighbors;
import eu.unitn.disi.db.exemplar.core.algorithms.EditDistanceQuerySearch;
import eu.unitn.disi.db.exemplar.core.algorithms.PruningAlgorithm;
import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.MappedNode;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.utils.Utilities;
import eu.unitn.disi.db.grava.vectorization.NeighborTables;
import eu.unitn.disi.db.mutilities.StopWatch;
import eu.unitn.disi.db.tool.ThreadPoolFactory;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

public class BFEXEDAlgorithm {

    private static final int AVG_DEGREE = 9;
    private static final int MAX_DEGREE = 688;
    private final int repititions;
    private final int threshold;
    private final int threadsNum;
    private final int neighbourNum;
    private final String queryName;
    private final Multigraph G;
    private final ComputeGraphNeighbors computeGraphNeighbors;

    public BFEXEDAlgorithm(int repititions, int threshold, int threadsNum,
                           int neighbourNum, Multigraph G, String queryName,
                           ComputeGraphNeighbors computeGraphNeighbors) {
        this.repititions = repititions;
        this.threshold = threshold;
        this.threadsNum = threadsNum;
        this.neighbourNum = neighbourNum;
        this.queryName = queryName;
        this.G = G;
        this.computeGraphNeighbors = computeGraphNeighbors;
    }

    public void runBFEXED() throws IOException, ParseException,
            AlgorithmExecutionException {
        long startTime = System.nanoTime();
        System.out.println("Running bf extension");
        Map<Long, Set<MappedNode>> queryGraphMapping = null;
        ComputeGraphNeighbors tableAlgorithm = null;
        NeighborTables queryTables = null;
        NeighborTables graphTables = null;
        PruningAlgorithm pruningAlgorithm = null;
        //NextQueryVertexes nqv = null;
        //Isomorphism iso = null;
        //float loadingTime = 0;
        //float computingNeighborTime = 0;
        //float pruningTime = 0;
        //float isoTime = 0;
        Utilities.searchCount = 0;
//		String comFile = "comparison.txt";
        //BufferedWriter comBw = new BufferedWriter(new FileWriter(comFile, true));
        Long startingNode;
//		Indexing ind = new Indexing();
//		Selectivity sel = new Selectivity();
//		ArrayList<InfoNode> infoNodes = new ArrayList<>();
//		this.exCandidatesNum = 0;
//		this.exAfterNum = 0;
        HashSet<RelatedQuery> relatedQueriesUnique = new HashSet<>();
        EditDistanceQuerySearch.answerCount = 0;
        StopWatch total = new StopWatch();
        total.start();
        EditDistanceQuerySearch edAlgorithm = new EditDistanceQuerySearch();
        for (int exprimentTime = 0; exprimentTime < repititions; exprimentTime++) {
            StopWatch watch = new StopWatch();
            watch.start();

            Multigraph Q = new BigMultigraph(queryName, queryName, true);

            tableAlgorithm = new ComputeGraphNeighbors();
            watch.reset();
            tableAlgorithm.setNodePool(ThreadPoolFactory.getTableComputeThreadPool());
            tableAlgorithm.setNumThreads(
                    ((ThreadPoolExecutor) ThreadPoolFactory.getTableComputeThreadPool()).getMaximumPoolSize());
            tableAlgorithm.setK(neighbourNum);
            /**
             tableAlgorithm.setGraph(G);
             tableAlgorithm.compute();
             tableAlgorithm.computePathFilter();
             **/
            graphTables = computeGraphNeighbors.getNeighborTables();
            tableAlgorithm.setGraph(Q);
//            tableAlgorithm.computePathFilter();

//            queryTables = tableAlgorithm.getNeighborTables();
            watch.reset();
            StartingNodeAlgorithm startingNodeAlgorithm = new StartingNodeBaseAlgorithm(G.getLabelFreq());
            List<Long> startingNodes = startingNodeAlgorithm.getStartingNodes(Q);
            //	InfoNode info = new InfoNode(startingNode);
            // System.out.println("starting node:" + startingNode);
            pruningAlgorithm = new PruningAlgorithm();
            // Set starting node according to sels of nodes.
            StopWatch detailedWatch = new StopWatch();

            System.out.println("starting: " + total.getElapsedTimeMillis());
            detailedWatch.start();
            for (int i = 0; i < 2; i++) {
                startingNode = startingNodes.get(i);
                System.out.println("Starting node:" + startingNode);
                pruningAlgorithm.setStartingNode(startingNode);
                pruningAlgorithm.setGraph(G);
                pruningAlgorithm.setQuery(Q);
                pruningAlgorithm.setK(this.neighbourNum);
                pruningAlgorithm.setGraphTables(graphTables);
                pruningAlgorithm.setQueryTables(queryTables);
                pruningAlgorithm.setThreshold(this.threshold);
                pruningAlgorithm.setgPathTables(computeGraphNeighbors.getPathTables());
                pruningAlgorithm.computeWithPath();
                System.out.println("Pruning takes " + total.getElapsedTimeMillis());

                queryGraphMapping = pruningAlgorithm.getQueryGraphMapping();
                queryGraphMapping.entrySet().forEach(en -> {
                    System.out.println(en.getKey() + ": " + en.getValue().size());
//                    en.getValue().forEach(val -> System.out.print(val.getNodeID() + ","));
//                    System.out.println();
                });

                watch.reset();

                List<RelatedQuery> relatedQueries;

                edAlgorithm.setStartingNode(startingNode);

                edAlgorithm.setQuery(Q);
                edAlgorithm.setGraph(pruningAlgorithm.pruneGraph());
                edAlgorithm.setQueryToGraphMap(pruningAlgorithm.getQueryGraphMapping());
                edAlgorithm.setLimitedComputation(false);
                edAlgorithm.setThreshold(threshold);
                edAlgorithm.compute();
                relatedQueriesUnique.addAll(edAlgorithm.getRelatedQueries());
                System.out.println("crt: " + i + " " + total.getElapsedTimeMillis());
//                System.out.println(queryName + " of " + i + " starting node " + startingNode + " takes " + detailedWatch
//                        .getElapsedTimeMillis()
//                        + " answer size:" + relatedQueriesUnique.size());
                detailedWatch.reset();
            }
        }

        System.out.println(queryName + " takes " + total.getElapsedTimeMillis()
                + " answer size:" + relatedQueriesUnique.size());
    }
}
