package eu.unitn.disi.db.grava.scc;

import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.command.util.StopWatch;
import eu.unitn.disi.db.exemplar.core.RelatedQuery;
import eu.unitn.disi.db.exemplar.core.algorithms.ComputeGraphNeighbors;
import eu.unitn.disi.db.exemplar.core.algorithms.IsomorphicQuerySearch;
import eu.unitn.disi.db.exemplar.core.algorithms.PruningAlgorithm;
import eu.unitn.disi.db.grava.exceptions.ParseException;
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
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class BothWildCardAlgorithm {
    private static final int AVG_DEGREE = 9;
    private static final int MAX_DEGREE = 688;
    private final int repititions;
    private final int threshold;
    private final int threadsNum;
    private final int neighbourNum;
    private final String graphName;
    private final String outputFile;
    private final String answerFile;
    private final String queryName;
    private Multigraph G;
    private Multigraph Q;
    private ComputeGraphNeighbors graphTableAlgorithm;

    public BothWildCardAlgorithm(int repititions, int threshold, int threadsNum,
                                 int neighbourNum, String graphName, String queryName,
                                 String outputFile, String answerFile) {
        this.repititions = repititions;
        this.threshold = threshold;
        this.threadsNum = threadsNum;
        this.neighbourNum = neighbourNum;
        this.graphName = graphName;
        this.queryName = queryName;
        this.outputFile = graphName + "_output/" + outputFile;
        this.answerFile = answerFile;
    }

    public void init(final Multigraph g)  {
        this.G = g;
    }
    
    public void runBothWildCard() throws IOException, AlgorithmExecutionException {
        long startTime = System.nanoTime();
        System.out.println("running wild card");
        int wcBsCount = 0;
        int wcCmpCount = 0;
        int wcUptCount = 0;
        long wcSearchCount = 0;
        Utilities.searchCount = 0;
        int edCandidates = 0;
        boolean isWcBad = false;
        Map<Long, Set<MappedNode>> queryGraphMapping = null;
        ComputeGraphNeighbors tableAlgorithm = null;
        // ComputePathGraphNeighbors tableAlgorithm = null;
        // PathNeighborTables queryTables = null;
        // PathNeighborTables graphTables = null;
        NeighborTables queryTables = null;
        NeighborTables graphTables = null;
        PruningAlgorithm pruningAlgorithm = null;
        NextQueryVertexes nqv = null;
        BufferedWriter comBw = null;
        Isomorphism iso = null;
        float loadingTime = 0;
        float computingNeighborTime = 0;
        float pruningTime = 0;
        float isoTime = 0;
        String temp[] = null;
        String comFile = "comparison.txt";
        int wcCandidatesNum = 0;
        int wcAfterNum = 0;
        int wcPathOnly = 0;
        int wcCost = 0;
        long wcIntNum = 0;
        long wcIntSum = 0;
        int wcAllEst = 0;
        int wcAdjEdt = 0;
        int wcPathEst = 0;
        StopWatch total = new StopWatch();
        total.start();
        if (threshold != 0) {
            HashSet<RelatedQuery> relatedQueriesUnique = new HashSet<>();
            WildCardQuery wcq = new WildCardQuery(threshold);
            wcq.run(queryName);
            Set<Multigraph> wildCardQueries = wcq.getWcQueries();
//			relatedQueriesUnique = new HashSet<>();
            for (int exprimentTime = 0; exprimentTime < repititions; exprimentTime++) {
                // ed.setThreshold(0);
                for (Multigraph wildCardQuery : wildCardQueries) {
                    System.out.println("===========");
					for (Edge e : wildCardQuery.edgeSet()){
                        System.out.println(e.getSource() + " " + e.getDestination() + " " + e.getLabel());
					}

                    this.setQ(wildCardQuery);
                    // System.out.println("queryfile:" + queryName);
//					comBw = new BufferedWriter(new FileWriter(comFile, true));
                    Long startingNode;
                    Indexing ind = new Indexing();
                    Selectivity sel = new Selectivity();
                    ArrayList<InfoNode> infoNodes = new ArrayList<>();
                    for (int exprimentTime1 = 0; exprimentTime1 < repititions; exprimentTime1++) {
                        StopWatch watch = new StopWatch();
                        watch.start();
                        loadingTime += watch.getElapsedTimeMillis();

                        tableAlgorithm = new ComputeGraphNeighbors();
                        watch.reset();
                        tableAlgorithm.setNumThreads(threadsNum);
                        tableAlgorithm.setK(neighbourNum);
                        tableAlgorithm.setMaxDegree(MAX_DEGREE);
                        /**
                         tableAlgorithm.setGraph(G);
                         tableAlgorithm.compute();
                         tableAlgorithm.computePathFilter();
                         **/
                        graphTables = graphTableAlgorithm.getNeighborTables();
                        tableAlgorithm.setGraph(wildCardQuery);
                        tableAlgorithm.setMaxDegree(this.MAX_DEGREE);
                        tableAlgorithm.compute();
//                        System.out.println("start pruninig");
                        queryTables = tableAlgorithm.getNeighborTables();
                        computingNeighborTime += watch.getElapsedTimeMillis();
                        // System.out.println(queryTables.toString());
                        watch.reset();
                        startingNode = this.getRootNode(true);
                        System.out.println("starting node:" + startingNode);
//						InfoNode info = new InfoNode(startingNode);
                        // System.out.println("starting node:" + startingNode);
                        pruningAlgorithm = new PruningAlgorithm();
                        // Set starting node according to sels of nodes.
                        pruningAlgorithm.setStartingNode(startingNode);
                        pruningAlgorithm.setGraph(G);
                        pruningAlgorithm.setQuery(Q);
                        pruningAlgorithm.setK(neighbourNum);
                        pruningAlgorithm.setgPathTables(graphTableAlgorithm.getPathTables());
                        pruningAlgorithm.setGraphTables(graphTables);
                        pruningAlgorithm.setQueryTables(queryTables);
//						System.out.println("startingNode:" + startingNode + " degree:" + (Q.inDegreeOf(startingNode) + Q.outDegreeOf(startingNode))); 
                        // pruningAlgorithm.setGraphPathTables(graphTables);
                        // pruningAlgorithm.setQueryPathTables(queryTables);
                        pruningAlgorithm.setThreshold(0);
                        pruningAlgorithm.compute();
                        pruningAlgorithm.pathFilter(true);

                        // pruningAlgorithm.fastCompute();
//						this.wcBsCount += pruningAlgorithm.getBsCount();
//						this.wcCmpCount += pruningAlgorithm.getCmpNbLabel();
//						this.wcUptCount += pruningAlgorithm.getUptCount();
//						info.setBsCount(pruningAlgorithm.getBsCount());
//						info.setCmpCount(pruningAlgorithm.getCmpNbLabel());
//						info.setUptCount(pruningAlgorithm.getUptCount());
                        // info.setSel(nqv.getNodeSelectivities().get(startingNode));
//						infoNodes.add(info);
                        queryGraphMapping = pruningAlgorithm.getQueryGraphMapping();
                        wcCandidatesNum += queryGraphMapping.get(startingNode).size();
//						System.out.println("before filtering:" + queryGraphMapping.get(startingNode).size());
//						pruningAlgorithm.pathFilter();
                        wcAfterNum += queryGraphMapping.get(startingNode).size();
//						this.wcPathOnly += pruningAlgorithm.onlyPath();
//						System.out.println("after filtering:" + queryGraphMapping.get(startingNode).size());
                        // watch.getElapsedTimeMillis());
                        pruningTime += watch.getElapsedTimeMillis();
                        watch.reset();
//                        System.out.println("start computing");
                        Set<RelatedQuery> relatedQueries;

                        IsomorphicQuerySearch edAlgorithm = new IsomorphicQuerySearch();
                        edAlgorithm.setStartingNode(startingNode);
                        edAlgorithm.setLabelFreq(((BigMultigraph) G).getLabelFreq());
                        edAlgorithm.setQuery(Q);
                        edAlgorithm.setGraph(pruningAlgorithm.pruneGraph());
                        edAlgorithm.setNumThreads(this.threadsNum);
                        edAlgorithm.setQueryToGraphMap(pruningAlgorithm.getQueryGraphMapping());
                        edAlgorithm.setLimitedComputation(false);
                        edAlgorithm.compute();
//						this.isWcBad = this.isWcBad || IsomorphicQuerySearch.isBad;
                        wcIntNum = Math.max(wcIntNum, IsomorphicQuerySearch.interNum);
                        wcIntSum = wcIntSum + IsomorphicQuerySearch.interNum;
                        relatedQueries = edAlgorithm.getRelatedQueries();
//                        relatedQueriesUnique.addAll(relatedQueries);
//                        this.printAnswer(relatedQueriesUnique);
//						System.out.println(startingNode);

//						QuerySel qs = new QuerySel(G, wildCardQuery, startingNode);
//						
                        Cost.cost = 0;
//						Cost.estimateMaxCost(wildCardQuery, startingNode, G, this.AVG_DEGREE, new HashSet<Edge>(), 1);
//						Cost.estimateMaxCostWithLabelMaxNum(wildCardQuery, startingNode, this.AVG_DEGREE, G, new HashSet<Edge>(), 1);
                        Cost.estimateExactCost(wildCardQuery, startingNode, AVG_DEGREE, G, new HashSet<Edge>(), 1);
                    }

//					comBw.close();
                    Q = null;

//					loadingTime = loadingTime / repititions;
//					computingNeighborTime = computingNeighborTime / repititions;
//					pruningTime = pruningTime / repititions;
//					isoTime = isoTime / repititions;

                    // relatedQueriesUnique.addAll(this.getRelatedQueriesUnique());
                    // bsCount += this.getBsCount();
                    // cmpCount += this.getCmpCount();
                    // uptCount += this.getUptCount();
                }
//				System.out.println("asd:" + wcCost);
//				int queriesCount = 0;
//				for (RelatedQuery related : relatedQueriesUnique) {
//					queriesCount++;
//				}
                wcSearchCount = Utilities.searchCount / repititions;
//				wcBsCount /= repititions;
//				wcCmpCount /= repititions;
//				wcUptCount /= repititions;
//				wcSearchCount /= repititions;
//				System.out.println("c:" + wcSearchCount);
//				System.out.println(wcBsCount);
//				System.out.println(wcCmpCount);
//				System.out.println(wcUptCount);
//				System.out.println(wcSearchCount);
            }
//			answerNum = relatedQueriesUnique.size();
//			System.out.println(this.wcIntNum + " " + this.wcIntSum);
        } else {

        }

        System.out.println(queryName + " takes " + total.getElapsedTimeMillis());

//        wcElapsedTime = (double) (System.nanoTime() - startTime) / 1000000000.0;
    }

    public Long getRootNode(boolean minimumFrquency) throws AlgorithmExecutionException {
        Collection<Long> nodes = this.Q.vertexSet();
        Set<Long> edgeLabels = new HashSet<>();
        int maxFreq = 0, tempFreq = 0;
        Long goodNode = null;

        for (Edge l : this.Q.edgeSet()) {
            edgeLabels.add(l.getLabel());
        }

        Long bestLabel = -1L;

//        for (Edge e : this.Q.edgeSet()) {
//            bestLabel = e.getLabel() > bestLabel ? e.getLabel() : bestLabel;
//        }

        bestLabel =this.findLessFrequentLabel(edgeLabels);

        if (bestLabel == null || bestLabel == -1L) {
            throw new AlgorithmExecutionException("Best Label not found when looking for a root node!");
        }

        Collection<Edge> edgesIn, edgesOut;

        for (Long concept : nodes) {
            tempFreq = this.Q.inDegreeOf(concept) + this.Q.outDegreeOf(concept);

            edgesIn = this.Q.incomingEdgesOf(concept);

            for (Edge Edge : edgesIn) {
                if (Edge.getLabel().equals(bestLabel)) {
                    if (tempFreq > maxFreq) {
                        goodNode = concept;
                        maxFreq = tempFreq;
                    }
                }
            }

            edgesOut = this.Q.outgoingEdgesOf(concept);
            for (Edge Edge : edgesOut) {
                if (Edge.getLabel().equals(bestLabel)) {
                    if (tempFreq > maxFreq) {
                        goodNode = concept;
                        maxFreq = tempFreq;
                    }
                }
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

    private void printAnswer(Collection<RelatedQuery> queries) {
        System.out.println("printing answers");
        int num = 1;
//		System.out.println(queries.get(0));
        System.out.println("========================================");
        for (RelatedQuery rq : queries) {
//			System.out.println("printing answer " + num);
//			EditDistanceQuery eq = (EditDistanceQuery) rq;
            Map<Edge, Edge> mappedEdges = rq.getMappedEdges();
            Map<Long, Long> answerNodes = new HashMap<>();
            boolean right = true;
            for (Entry<Edge, Edge> en : mappedEdges.entrySet()) {
                Edge queryEdge = en.getKey();
                Edge graphEdge = en.getValue();
                if (queryEdge.getSource() < 0) {
                    if (!queryEdge.getSource().equals(-graphEdge.getSource())) {
                        right = false;
                        break;
                    }
                } else {
                    answerNodes.put(queryEdge.getSource(), graphEdge.getSource());
                }
                if (queryEdge.getDestination() < 0) {
                    if (!queryEdge.getDestination().equals(-graphEdge.getDestination())) {
                        right = false;
                        break;
                    }
                } else {
                    answerNodes.put(queryEdge.getDestination(), graphEdge.getDestination());
                }
            }
            if (right) {
                System.out.println("Printing answer " + num++);
//				for (Entry<Long, Long> en : answerNodes.entrySet()) {
//					System.out.println(this.id2Entity.get(en.getKey()).trim() + "=>" + this.id2Entity.get(en.getValue()).trim());
//					System.out.println(en.getKey() + "=>" + en.getValue());
//				}

                for (Edge e : mappedEdges.values()) {
//					System.out.println(this.id2Entity.get(e.getSource()).trim() + " " + this.id2Predicate.get(e.getLabel()).trim() + " " + this.id2Entity.get(e.getDestination()).trim());
                    System.out.println(e.getSource() + " " + e.getDestination() + " " + e.getLabel());
                }
                System.out.println("========================================");
            }
//			answers.add(this.id2Entity.get(eq.getNodesMapping().get(this.nodeToSearch).getNodeID()).trim());
        }
//		System.out.println("querying " + this.id2Entity.get(this.nodeToSearch));
//		for (String answer : answers) {
//			System.out.println(answer);
//		}
//		System.out.println("answer number:" + answers.size());
    }

    public void setQ(final Multigraph q) {
        Q = q;
    }

    public void setGraphTableAlgorithm(final ComputeGraphNeighbors graphTableAlgorithm) {
        this.graphTableAlgorithm = graphTableAlgorithm;
    }
}
