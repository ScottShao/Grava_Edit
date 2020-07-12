package eu.unitn.disi.db.grava.scc;

import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.command.util.StopWatch;
import eu.unitn.disi.db.exemplar.core.RelatedQuery;
import eu.unitn.disi.db.exemplar.core.algorithms.ComputeGraphNeighbors;
import eu.unitn.disi.db.exemplar.core.algorithms.IsomorphicQuerySearch;
import eu.unitn.disi.db.exemplar.core.algorithms.PruningAlgorithm;
import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.MappedNode;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.vectorization.NeighborTables;
import eu.unitn.disi.db.query.WildCardQuery;
import eu.unitn.disi.db.tool.ThreadPoolFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public class WildCardAlgorithm {

    private static final int AVG_DEGREE = 9;
    private static final int MAX_DEGREE = 688;
    private final int repititions;
    private final int threshold;
    private final int threadsNum;
    private final int neighbourNum;
    private final String queryName;
    private final Multigraph G;
    private final NeighborTables graphNeighborTable;

    public WildCardAlgorithm(int repititions, int threshold, int threadsNum,
                             int neighbourNum, Multigraph G, String queryName,
                             NeighborTables graphNeighborTable) {
        this.repititions = repititions;
        this.threshold = threshold;
        this.threadsNum = threadsNum;
        this.neighbourNum = neighbourNum;
        this.queryName = queryName;
        this.G = G;
        this.graphNeighborTable = graphNeighborTable;
    }

    public void runWildCard(ExecutorService executorService) throws IOException, ParseException,
            AlgorithmExecutionException {
        // ComputePathGraphNeighbors tableAlgorithm = null;
        // PathNeighborTables queryTables = null;
        // PathNeighborTables graphTables = null;

        StopWatch total = new StopWatch();
        total.start();
        Set<RelatedQuery> relatedQueries = new HashSet<>();
        if (threshold != 0) {
            HashSet<RelatedQuery> relatedQueriesUnique = new HashSet<>();
            WildCardQuery wcq = new WildCardQuery(threshold);
            wcq.run(queryName);
            Set<Multigraph> wildCardQueries = new LinkedHashSet<>(wcq.getWcQueries());
//            for (Multigraph multigraph : wildCardQueries) {
//                System.out.print(multigraph.toString());
//                System.out.println("========================");
//            }

            Iterator<Multigraph> iterator = wildCardQueries.iterator();
            int size = wildCardQueries.size();
            List<CompletableFuture<Set<RelatedQuery>>> tasks = new ArrayList<>();
            for (int i = 0; i < size; i++) {

                    Multigraph currentQuery = iterator.next();
                    CompletableFuture<Set<RelatedQuery>> task = new CompletableFuture<>();
                    Callable<Set<RelatedQuery>> work = createWork(G, currentQuery);
                    executorService.execute(() -> {
                        try {
                            task.complete(work.call());
                        } catch (Throwable exception) {
                            task.completeExceptionally(exception);
                        }
                    });
                    tasks.add(task);
            }
            try {
                CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[0])).join();
                for (CompletableFuture<Set<RelatedQuery>> task : tasks) {
                    relatedQueries.addAll(task.get());
                    Set<RelatedQuery> results =  task.get();
                }

            } catch (InterruptedException e) {
                throw new RuntimeException("ie ", e);
            } catch (ExecutionException e) {
                throw new RuntimeException("ee", e);
            }
        } else {

        }

        System.out.println(queryName + " total takes " + total.getElapsedTimeMillis()
                + " answer size:" + relatedQueries.size());
//        AnswerManagement.printAnswer(relatedQueries);
    }

    private Callable<Set<RelatedQuery>> createWork(Multigraph graph, Multigraph wildCardQuery)
            throws AlgorithmExecutionException {
        return () -> {

            Long startingNode;
            IsomorphicQuerySearch edAlgorithm = new IsomorphicQuerySearch();
            for (int exprimentTime1 = 0; exprimentTime1 < repititions; exprimentTime1++) {
                StopWatch watch = new StopWatch();
                watch.start();

                ComputeGraphNeighbors tableAlgorithm = new ComputeGraphNeighbors();
                tableAlgorithm.setNumThreads(threadsNum);
                tableAlgorithm.setK(neighbourNum);
                tableAlgorithm.setMaxDegree(MAX_DEGREE);
                tableAlgorithm.setNodePool(ThreadPoolFactory.getTableComputeThreadPool());

                tableAlgorithm.setGraph(wildCardQuery);
                tableAlgorithm.compute();
                System.out.println(queryName + " Compute neighbour takes " + watch.getElapsedTimeMillis());
                NeighborTables queryTables = tableAlgorithm.getNeighborTables();
                watch.reset();
                startingNode = this.getRootNode(wildCardQuery, true);
                System.out.println("starting node:" + startingNode);
//						InfoNode info = new InfoNode(startingNode);
                // System.out.println("starting node:" + startingNode);
                PruningAlgorithm pruningAlgorithm = new PruningAlgorithm();
                // Set starting node according to sels of nodes.
                pruningAlgorithm.setStartingNode(startingNode);
                pruningAlgorithm.setGraph(graph);
                pruningAlgorithm.setQuery(wildCardQuery);
                pruningAlgorithm.setK(neighbourNum);
                pruningAlgorithm.setGraphTables(graphNeighborTable);
                pruningAlgorithm.setQueryTables(queryTables);
                pruningAlgorithm.setThreshold(0);
                pruningAlgorithm.compute();
                System.out.println(queryName + "pruning takes " + watch.getElapsedTimeMillis());

                Map<Long, Set<MappedNode>> queryGraphMapping = pruningAlgorithm.getQueryGraphMapping();

                queryGraphMapping.entrySet().forEach(en -> {
                    System.out.println(en.getKey() + ": " + en.getValue().size());
                    en.getValue().forEach(val -> System.out.print(val.getNodeID() + ","));
                    System.out.println();
                });
                watch.reset();

                try {
                    edAlgorithm.setStartingNode(startingNode);
                    edAlgorithm.setLabelFreq((graph).getLabelFreq());
                    edAlgorithm.setQuery(wildCardQuery);
                    edAlgorithm.setGraph(pruningAlgorithm.pruneGraph());
                    edAlgorithm.setNumThreads(this.threadsNum);
                    edAlgorithm.setQueryToGraphMap(pruningAlgorithm.getQueryGraphMapping());
                    edAlgorithm.setLimitedComputation(false);
                    edAlgorithm.setExecutionPool(ThreadPoolFactory.getSearchThreadPool());
                    edAlgorithm.compute();
                } catch (AlgorithmExecutionException e) {
                    System.out.println("testing exception" + graph);
                }
            }
            return edAlgorithm.getRelatedQueries();
        };
    }

    public Long getRootNode(Multigraph wildcardQuery, boolean minimumFrquency) throws AlgorithmExecutionException {
        Collection<Long> nodes = wildcardQuery.vertexSet();
        Set<Long> edgeLabels = new HashSet<>();
        int maxFreq = 0, tempFreq = 0;
        Long goodNode = null;

        for (Edge l : wildcardQuery.edgeSet()) {
            edgeLabels.add(l.getLabel());
        }

        Long bestLabel = -1L;

//        for (Edge e : this.Q.edgeSet()) {
//            bestLabel = e.getLabel() > bestLabel ? e.getLabel() : bestLabel;
//        }

        bestLabel = this.findLessFrequentLabel(edgeLabels);

        if (bestLabel == null || bestLabel == -1L) {
            throw new AlgorithmExecutionException("Best Label not found when looking for a root node!");
        }

        Collection<Edge> edgesIn, edgesOut;

        for (Long concept : nodes) {
            tempFreq = wildcardQuery.inDegreeOf(concept) + wildcardQuery.outDegreeOf(concept);
            if (tempFreq > maxFreq) {
                goodNode = concept;
                maxFreq = tempFreq;
            }
        }

        return goodNode;
    }

    private Long findLessFrequentLabel(final Set<Long> edgeLabels) {
        Long candidate = null;
        int freq = Integer.MAX_VALUE;
        for (Long label : edgeLabels) {
            if (label.equals(0L)) {
                continue;
            }
            if (G.getLabelFreq().get(label).getFrequency() < freq) {
                candidate = label;
            }
        }
        return candidate;
    }
}
