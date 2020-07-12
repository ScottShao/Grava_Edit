package eu.unitn.disi.db.grava.scc;

import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.command.util.StopWatch;
import eu.unitn.disi.db.exemplar.core.RelatedQuery;
import eu.unitn.disi.db.exemplar.core.StartingNodeAlgorithm;
import eu.unitn.disi.db.exemplar.core.StartingNodeBaseAlgorithm;
import eu.unitn.disi.db.exemplar.core.algorithms.ComputeGraphNeighbors;
import eu.unitn.disi.db.exemplar.core.algorithms.EditDistanceQuerySearch;
import eu.unitn.disi.db.exemplar.core.algorithms.IsomorphicQuerySearch;
import eu.unitn.disi.db.exemplar.core.algorithms.PruningAlgorithm;
import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.Answer;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Indexing;
import eu.unitn.disi.db.grava.graphs.InfoNode;
import eu.unitn.disi.db.grava.graphs.MappedNode;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.graphs.Selectivity;
import eu.unitn.disi.db.grava.utils.Utilities;
import eu.unitn.disi.db.grava.vectorization.NeighborTables;
import eu.unitn.disi.db.query.WildCardQuery;
import eu.unitn.disi.db.tool.AnswerManagement;
import eu.unitn.disi.db.tool.ThreadPoolFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

public class EXEDAlgorithm {

    private static final int AVG_DEGREE = 9;
    private static final int MAX_DEGREE = 688;
    private final int repititions;
    private final int threshold;
    private final int threadsNum;
    private final int neighbourNum;
    private final String queryName;
    private final Multigraph G;
    private final NeighborTables graphNeighborTable;

    public EXEDAlgorithm(int repititions, int threshold, int threadsNum,
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

    public void runEXED() throws IOException, ParseException,
            AlgorithmExecutionException {
        long startTime = System.nanoTime();
        System.out.println("Running extension");
        Map<Long, Set<MappedNode>> queryGraphMapping = null;
        ComputeGraphNeighbors tableAlgorithm = null;
        NeighborTables queryTables = null;
        PruningAlgorithm pruningAlgorithm = null;
//		NextQueryVertexes nqv = null;
//		Isomorphism iso = null;
//		float loadingTime = 0;
//		float computingNeighborTime = 0;
//		float pruningTime = 0;
//		float isoTime = 0;
        Utilities.searchCount = 0;
        String comFile = "comparison.txt";
//		BufferedWriter comBw = new BufferedWriter(new FileWriter(comFile, true));
        List<Long> startingNodes;
        Indexing ind = new Indexing();
        Selectivity sel = new Selectivity();
        ArrayList<InfoNode> infoNodes = new ArrayList<>();
        HashSet<RelatedQuery> relatedQueriesUnique = new HashSet<>();
        eu.unitn.disi.db.mutilities.StopWatch total = new eu.unitn.disi.db.mutilities.StopWatch();
        total.start();
        EditDistanceQuerySearch edAlgorithm = new EditDistanceQuerySearch();
        for (int exprimentTime = 0; exprimentTime < repititions; exprimentTime++) {

            Multigraph Q = new BigMultigraph(queryName, queryName, true);

            tableAlgorithm = new ComputeGraphNeighbors();
            tableAlgorithm.setNumThreads(((ThreadPoolExecutor)ThreadPoolFactory.getTableComputeThreadPool()).getMaximumPoolSize());
            tableAlgorithm.setK(neighbourNum);
            tableAlgorithm.setNodePool(ThreadPoolFactory.getTableComputeThreadPool());
            /**
             tableAlgorithm.setGraph(G);
             tableAlgorithm.compute();
             tableAlgorithm.computePathFilter();
             **/
            tableAlgorithm.setGraph(Q);
            tableAlgorithm.compute();
            queryTables = tableAlgorithm.getNeighborTables();
//			computingNeighborTime += watch.getElapsedTimeMillis();
//			startingNode = this.getRootNode(true);
            StartingNodeAlgorithm startingNodeAlgorithm = new StartingNodeBaseAlgorithm(G.getLabelFreq());
            startingNodes = startingNodeAlgorithm.getStartingNodes(Q);

//			InfoNode info = new InfoNode(startingNode);
            // System.out.println("starting node:" + startingNode);
            pruningAlgorithm = new PruningAlgorithm();
            // Set starting node according to sels of nodes.
            StopWatch detailedWatch = new StopWatch();
            for (int i = 0; i < 2 ; i++) {
                detailedWatch.start();
                Long startingNode = startingNodes.get(i);
                System.out.println("Starting node: " + startingNode);
                pruningAlgorithm.setStartingNode(startingNode);
                pruningAlgorithm.setGraph(G);
                pruningAlgorithm.setQuery(Q);
                pruningAlgorithm.setK(this.neighbourNum);
                pruningAlgorithm.setGraphTables(graphNeighborTable);
                pruningAlgorithm.setQueryTables(queryTables);
                pruningAlgorithm.setThreshold(this.threshold);
//			pruningAlgorithm.setgPathTables(gTableAlgorithm.getPathTables());
                pruningAlgorithm.compute();
                System.out.println("Pruning takes " + detailedWatch.getElapsedTimeMillis());
//			this.exBsCount = pruningAlgorithm.getBsCount();
//			this.exCmpCount = pruningAlgorithm.getCmpNbLabel();
//			this.exUptCount = pruningAlgorithm.getUptCount();
//			info.setBsCount(pruningAlgorithm.getBsCount());
//			info.setCmpCount(pruningAlgorithm.getCmpNbLabel());
//			info.setUptCount(pruningAlgorithm.getUptCount());
//			infoNodes.add(info);
                queryGraphMapping = pruningAlgorithm.getQueryGraphMapping();
                queryGraphMapping.entrySet().forEach(en -> {
                    System.out.println(en.getKey() + ": " + en.getValue().size());
//                    en.getValue().forEach(val -> System.out.print(val.getNodeID() + ","));
//                    System.out.println();
                });
//			this.exCandidatesNum = queryGraphMapping.get(startingNode).size();
//			System.out.println("ex before:" + exCandidatesNum);
//			pruningAlgorithm.pathFilter();
//			this.exAfterNum = queryGraphMapping.get(startingNode).size();
//			this.exPathOnly = pruningAlgorithm.onlyPath();
//			System.out.println("ex after:" + queryGraphMapping.get(startingNode).size());
//			pruningTime += watch.getElapsedTimeMillis();

                List<RelatedQuery> relatedQueries;

                edAlgorithm.setStartingNode(startingNode);

                edAlgorithm.setQuery(Q);
                edAlgorithm.setGraph(pruningAlgorithm.pruneGraph());
//                edAlgorithm.setNumThreads(this.threadsNum);
                edAlgorithm.setQueryToGraphMap(pruningAlgorithm.getQueryGraphMapping());
                edAlgorithm.setLimitedComputation(true);
                edAlgorithm.setThreshold(threshold);
                edAlgorithm.compute();
                relatedQueriesUnique.addAll(edAlgorithm.getRelatedQueries());
                System.out.println("crt: " + i + " " + total.getElapsedTimeMillis());
//                System.out.println(queryName + " of " + i + " starting node " + startingNode + " takes " + detailedWatch.getElapsedTimeMillis()
//                        + " answer size:" + relatedQueriesUnique.size());
                detailedWatch.reset();
            }
        }
        System.out.println(queryName + " takes " + total.getElapsedTimeMillis()
                + " answer size:" + relatedQueriesUnique.size());
    }

    public List<Long> getRootNode(Multigraph Q, boolean minimumFrquency) throws AlgorithmExecutionException {
        Map<Long, Integer> freqMap = new HashMap<>();
        Collection<Long> nodes = Q.vertexSet();
        Set<Long> edgeLabels = new HashSet<>();
        int maxFreq = 0, tempFreq = 0;
        Long goodNode = null;

        for (Edge l : Q.edgeSet()) {
            edgeLabels.add(l.getLabel());
        }

        Long bestLabel = -1L;

        bestLabel =this.findLessFrequentLabel(edgeLabels);

        if (bestLabel == null || bestLabel == -1L) {
            throw new AlgorithmExecutionException("Best Label not found when looking for a root node!");
        }

        Collection<Edge> edgesIn, edgesOut;

        for (Long concept : nodes) {
            tempFreq = Q.inDegreeOf(concept) + Q.outDegreeOf(concept);
            for (Edge e: Q.incomingEdgesOf(concept)) {
                Long nextNode = e.getDestination().equals(concept) ? e.getSource() : e.getDestination();
                tempFreq += Q.inDegreeOf(nextNode) + Q.outDegreeOf(nextNode);
            }
            for (Edge e: Q.outgoingEdgesOf(concept)) {
                Long nextNode = e.getDestination().equals(concept) ? e.getSource() : e.getDestination();
                tempFreq += Q.inDegreeOf(nextNode) + Q.outDegreeOf(nextNode);
            }
            freqMap.put(concept, tempFreq);
        }
        List<Long> results=  freqMap.entrySet().stream().sorted(Map.Entry.comparingByValue()).map(Entry::getKey).collect(Collectors.toList());
        return results;
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
