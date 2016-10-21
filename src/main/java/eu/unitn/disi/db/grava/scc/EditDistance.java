package eu.unitn.disi.db.grava.scc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Map.Entry;
import java.util.Set;

import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.exemplar.core.EditDistanceQuery;
import eu.unitn.disi.db.exemplar.core.IsomorphicQuery;
import eu.unitn.disi.db.exemplar.core.RelatedQuery;
import eu.unitn.disi.db.exemplar.core.algorithms.ComputeGraphNeighbors;
import eu.unitn.disi.db.exemplar.core.algorithms.ComputePathGraphNeighbors;
import eu.unitn.disi.db.exemplar.core.algorithms.EditDistanceQuerySearch;
import eu.unitn.disi.db.exemplar.core.algorithms.IsomorphicQuerySearch;
import eu.unitn.disi.db.exemplar.core.algorithms.PruningAlgorithm;
import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.BaseMultigraph;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Indexing;
import eu.unitn.disi.db.grava.graphs.InfoNode;
import eu.unitn.disi.db.grava.graphs.LabelContainer;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.graphs.Selectivity;
import eu.unitn.disi.db.grava.utils.Convertion;
import eu.unitn.disi.db.grava.utils.FileOperator;
import eu.unitn.disi.db.grava.utils.MethodOption;
import eu.unitn.disi.db.grava.utils.NodeCostComparator;
import eu.unitn.disi.db.grava.utils.NodeExSelComparator;
import eu.unitn.disi.db.grava.utils.NodeSelComparator;
import eu.unitn.disi.db.grava.utils.Utilities;
import eu.unitn.disi.db.grava.vectorization.NeighborTables;
import eu.unitn.disi.db.grava.vectorization.PathNeighborTables;
import eu.unitn.disi.db.grava.graphs.MappedNode;
import eu.unitn.disi.db.mutilities.StopWatch;
import eu.unitn.disi.db.query.WildCardQuery;

public class EditDistance {
	private int repititions;
	private int threshold;
	private int threadsNum;
	private int neighbourNum;
	private String graphName;
	private String queryName;
	private String outputFile;
	private String answerFile;
	private Multigraph G;
	private Multigraph Q;
	private HashSet<RelatedQuery> relatedQueriesUnique;
	private long wcBsCount;
	private int wcCmpCount;
	private int wcUptCount;
	private long bfSearchCount;
	private long exBFSearchCount;
	private long wcSearchCount;
	private long exBsCount;
	private int exCmpCount;
	private int exUptCount;
	private long exSearchCount;
	private MethodOption mo;
	private BufferedWriter cmpBw;
	private double wcElapsedTime;
	private double exElapsedTime;
	private double bfElapsedTime;
	private double exBFTime;
	private int wcCandidatesNum;
	private int exCandidatesNum;
	private int wcAfterNum;
	private int exAfterNum;
	private int wcPathOnly;
	private int exPathOnly;
	private double wcCost;
	private double edCost;
	private final int AVG_DEGREE = 9;
	private final int MAX_DEGREE = 688;
	private int count = 0;
	private long answerNum;
	private List<String> strList;
	private double all;
	private double path;
	private double adj;
	private double edCandidates;
	private boolean isEdBad;
	private boolean isWcBad;
	private long wcIntNum;
	private long edIntNum;
	private long wcIntSum;
	private long bfIntNum;
	private long bfIntSum;
	private List<String> candComp;
	private List<String> selsComp;
	private ComputeGraphNeighbors gTableAlgorithm;
	private double wcAllEst;
	private double wcPathEst;
	private double wcAdjEdt;
	private double edAllEst;
	private double edPathEst;
	private double edAdjEdt;

	public BufferedWriter getCmpBw() {
		return cmpBw;
	}

	public void setCmpBw(BufferedWriter cmpBw) {
		this.cmpBw = cmpBw;
	}

	public EditDistance() throws AlgorithmExecutionException, ParseException,
			IOException {
	}

	public EditDistance(int repititions, int threshold, int threadsNum,
			int neighbourNum, String graphName, String queryName,
			String outputFile, String answerFile)
			throws AlgorithmExecutionException, ParseException, IOException {
		this.repititions = repititions;
		this.threshold = threshold;
		this.threadsNum = threadsNum;
		this.neighbourNum = neighbourNum;
		this.graphName = graphName;
		this.queryName = queryName;
		this.outputFile = graphName + "_output/" + outputFile;
		this.answerFile = answerFile;
		
	}

	public void runWildCard() throws IOException, ParseException,
			AlgorithmExecutionException {
		long startTime = System.nanoTime();
		System.out.println("running wild card");
		wcBsCount = 0;
		wcCmpCount = 0;
		wcUptCount = 0;
		wcSearchCount = 0;
		Utilities.searchCount = 0;
		this.edCandidates = 0;
		this.isWcBad = false;
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
		wcCandidatesNum = 0;
		this.wcAfterNum = 0;
		this.wcPathOnly = 0;
		wcCost = 0;
		this.wcIntNum = 0;
		this.wcIntSum = 0;
		this.wcAllEst = 0;
		this.wcAdjEdt = 0;
		this.wcPathEst = 0;
		if (threshold != 0) {
			HashSet<RelatedQuery> relatedQueriesUnique = new HashSet<RelatedQuery>();
			WildCardQuery wcq = new WildCardQuery(threshold);
			wcq.run(queryName);
			Set<Multigraph> wildCardQueries = wcq.getWcQueries();
//			relatedQueriesUnique = new HashSet<>();
			for (int exprimentTime = 0; exprimentTime < repititions; exprimentTime++) {
				// ed.setThreshold(0);
				for (Multigraph wildCardQuery : wildCardQueries) {
//					System.out.println("wc query");
//					for (Edge e : wildCardQuery.edgeSet()){
//						System.out.println(e.getSource() + " " + e.getDestination() + " " + e.getLabel());
//					}
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
						tableAlgorithm.setMaxDegree(this.MAX_DEGREE);
						/**
						tableAlgorithm.setGraph(G);
						tableAlgorithm.compute();
						tableAlgorithm.computePathFilter();
						**/
						graphTables = gTableAlgorithm.getNeighborTables();
						tableAlgorithm.setGraph(Q);
						tableAlgorithm.setMaxDegree(this.MAX_DEGREE);
						tableAlgorithm.compute();
						
						queryTables = tableAlgorithm.getNeighborTables();
						computingNeighborTime += watch.getElapsedTimeMillis();
						// System.out.println(queryTables.toString());
						watch.reset();
						watch.getElapsedTimeSecs();
						startingNode = this.getRootNode(true);
//						InfoNode info = new InfoNode(startingNode);
						// System.out.println("starting node:" + startingNode);
						pruningAlgorithm = new PruningAlgorithm();
						// Set starting node according to sels of nodes.
						pruningAlgorithm.setStartingNode(startingNode);
						pruningAlgorithm.setGraph(G);
						pruningAlgorithm.setQuery(Q);
						pruningAlgorithm.setK(neighbourNum);
						pruningAlgorithm.setgPathTables(gTableAlgorithm.getPathTables());
						pruningAlgorithm.setGraphTables(graphTables);
						pruningAlgorithm.setQueryTables(queryTables);
//						System.out.println("startingNode:" + startingNode + " degree:" + (Q.inDegreeOf(startingNode) + Q.outDegreeOf(startingNode))); 
						// pruningAlgorithm.setGraphPathTables(graphTables);
						// pruningAlgorithm.setQueryPathTables(queryTables);
						
						pruningAlgorithm.setThreshold(0);
						pruningAlgorithm.compute();
						
						
						// pruningAlgorithm.fastCompute();
//						this.wcBsCount += pruningAlgorithm.getBsCount();
//						this.wcCmpCount += pruningAlgorithm.getCmpNbLabel();
//						this.wcUptCount += pruningAlgorithm.getUptCount();
//						info.setBsCount(pruningAlgorithm.getBsCount());
//						info.setCmpCount(pruningAlgorithm.getCmpNbLabel());
//						info.setUptCount(pruningAlgorithm.getUptCount());
						// info.setSel(nqv.getNodeSelectivities().get(startingNode));
//						infoNodes.add(info);
						queryGraphMapping = pruningAlgorithm
								.getQueryGraphMapping();
						wcCandidatesNum += queryGraphMapping.get(startingNode).size();
//						System.out.println("before filtering:" + queryGraphMapping.get(startingNode).size());
//						pruningAlgorithm.pathFilter();
						this.wcAfterNum += queryGraphMapping.get(startingNode).size();
//						this.wcPathOnly += pruningAlgorithm.onlyPath();
//						System.out.println("after filtering:" + queryGraphMapping.get(startingNode).size());
						// watch.getElapsedTimeMillis());
						pruningTime += watch.getElapsedTimeMillis();
						watch.reset();

						List<RelatedQuery> relatedQueries;

						IsomorphicQuerySearch edAlgorithm = new IsomorphicQuerySearch();
						edAlgorithm.setStartingNode(startingNode);
						edAlgorithm.setLabelFreq(((BigMultigraph)G).getLabelFreq());
						edAlgorithm.setQuery(Q);
						edAlgorithm.setGraph(G);
						edAlgorithm.setNumThreads(this.threadsNum);
						edAlgorithm.setQueryToGraphMap(pruningAlgorithm
								.getQueryGraphMapping());
						edAlgorithm.setLimitedComputation(false);
						edAlgorithm.compute();
//						this.isWcBad = this.isWcBad || IsomorphicQuerySearch.isBad;
						this.wcIntNum = Math.max(this.wcIntNum, IsomorphicQuerySearch.interNum);
						this.wcIntSum = this.wcIntSum + IsomorphicQuerySearch.interNum;
//						relatedQueries = edAlgorithm.getRelatedQueries();
//						relatedQueriesUnique.addAll(relatedQueries);
//						System.out.println(startingNode);
						
//						QuerySel qs = new QuerySel(G, wildCardQuery, startingNode);
//						
						Cost.cost = 0;
//						Cost.estimateMaxCost(wildCardQuery, startingNode, G, this.AVG_DEGREE, new HashSet<Edge>(), 1);
//						Cost.estimateMaxCostWithLabelMaxNum(wildCardQuery, startingNode, this.AVG_DEGREE, G, new HashSet<Edge>(), 1);
						Cost.estimateExactCost(wildCardQuery, startingNode, this.AVG_DEGREE, G, new HashSet<Edge>(), 1);
//						System.out.println(qs.getCanNumber(startingNode, this.AVG_DEGREE, 2));
//						double tt = qs.getCanNumber(startingNode, this.AVG_DEGREE, 2);
//						this.edCandidates += tt;
//						wcCost += tt* Cost.cost;
//						wcCost += qs.getCanNumber(startingNode, AVG_DEGREE, 2) * Cost.cost;
//						wcCost += Cost.getCandidatesNum(wildCardQuery, startingNode, G) * Cost.cost;
//						wcCost += qs.computeAdjNotCand(startingNode, AVG_DEGREE, this.neighbourNum) * Cost.cost;
//						this.wcAllEst += qs.getCanNumber(startingNode, this.AVG_DEGREE, this.neighbourNum) * Cost.cost;
//						this.wcAdjEdt += qs.computeAdjNotCand(startingNode, AVG_DEGREE, this.neighbourNum) * Cost.cost;
//						this.wcPathEst += qs.computePathNotCand(startingNode, AVG_DEGREE, this.neighbourNum) * Cost.cost;
//						System.out.println(tt + " wc " + Cost.cost);
//						wcCost += Cost.getCandidatesNum(wildCardQuery, startingNode, G) * Cost.cost;
//						wcCost += Cost.estimateQueryCost(wildCardQuery, startingNode, G, AVG_DEGREE);
//						System.out.println(wcCost + "  asd");
//						comBw.newLine();
//						answerNum += IsomorphicQuerySearch.answerCount;
//						System.out.println(answerNum);
//						isoTime += watch.getElapsedTimeMillis();
						
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
		
		wcElapsedTime = (double)(System.nanoTime() - startTime) / 1000000000.0;
	}
	
	
	public void runBFWildCard() throws IOException, ParseException,
	AlgorithmExecutionException {
		long startTime = System.nanoTime();
		System.out.println("running bf wild card");
		Map<Long, Set<MappedNode>> queryGraphMapping = null;
		ComputeGraphNeighbors tableAlgorithm = null;
		// ComputePathGraphNeighbors tableAlgorithm = null;
		// PathNeighborTables queryTables = null;
		// PathNeighborTables graphTables = null;
		NeighborTables queryTables = null;
		NeighborTables graphTables = null;
		PruningAlgorithm pruningAlgorithm = null;
//		float loadingTime = 0;
//		float computingNeighborTime = 0;
//		float pruningTime = 0;
//		float isoTime = 0;
		Utilities.searchCount = 0;
		HashSet<RelatedQuery> relatedQueriesUnique = null;
		this.bfIntNum = 0;
		this.bfIntSum = 0;
		Q = new BigMultigraph(queryName, queryName, true);
		if (!this.isQueryMappable(Q)) {
			return;
		}
		Convertion con = new Convertion();
		Map<Long, Integer> labelCount = new HashMap<>();
		for (Edge e : Q.edgeSet()) {
			if (labelCount.containsKey(e.getLabel())) {
				labelCount.put(e.getLabel(), labelCount.get(e.getLabel()) + 1);
			} else {
				labelCount.put(e.getLabel(), 1);
			}
		}
		System.out.println("Original query:");
//		String temp = con.toReadable(Q.edgeSet());
//		if (temp == null) {
//			System.out.println("null");
//			return;
//		}
//		System.out.print(temp);
//		for (Edge e : Q.edgeSet()) {
//			System.out.println(e.getSource() + " " + e.getLabel() + " " + e.getDestination());
//			System.out.println(con.toReadable(e));
//		}
		System.out.println("==================");
		System.out.println("Anwser Set:");
		double start = System.nanoTime();
		if (threshold != 0) {
			relatedQueriesUnique = new HashSet<>();
			WildCardQuery wcq = new WildCardQuery(threshold);
			wcq.run(queryName);
			Set<Multigraph> wildCardQueries = wcq.getWcQueries();
		//	relatedQueriesUnique = new HashSet<>();
			for (int exprimentTime = 0; exprimentTime < repititions; exprimentTime++) {
				// ed.setThreshold(0);
				for (Multigraph wildCardQuery : wildCardQueries) {
		//			for (Edge e : wildCardQuery.edgeSet()){
		//				System.out.println(e);
		//			}
					this.setQ(wildCardQuery);
					Long startingNode;
//					ArrayList<InfoNode> infoNodes = new ArrayList<>();
					for (int exprimentTime1 = 0; exprimentTime1 < repititions; exprimentTime1++) {
						StopWatch watch = new StopWatch();
						watch.start();
		
//						loadingTime += watch.getElapsedTimeMillis();
		
						tableAlgorithm = new ComputeGraphNeighbors();
						watch.reset();
						tableAlgorithm.setNumThreads(threadsNum);
						tableAlgorithm.setK(neighbourNum);
						tableAlgorithm.setMaxDegree(this.MAX_DEGREE);
						/**
						tableAlgorithm.setGraph(G);
						tableAlgorithm.compute();
						tableAlgorithm.computePathFilter();
						**/
						graphTables = gTableAlgorithm.getNeighborTables();
						tableAlgorithm.setGraph(Q);
						tableAlgorithm.setMaxDegree(this.MAX_DEGREE);
						tableAlgorithm.compute();
						
						queryTables = tableAlgorithm.getNeighborTables();
//						computingNeighborTime += watch.getElapsedTimeMillis();
						// System.out.println(queryTables.toString());
						watch.reset();
						startingNode = this.getRootNode(true);
						// System.out.println("starting node:" + startingNode);
						pruningAlgorithm = new PruningAlgorithm();
						// Set starting node according to sels of nodes.
						pruningAlgorithm.setStartingNode(startingNode);
						pruningAlgorithm.setGraph(G);
						pruningAlgorithm.setQuery(Q);
						pruningAlgorithm.setK(neighbourNum);
						pruningAlgorithm.setgPathTables(gTableAlgorithm.getPathTables());
						pruningAlgorithm.setGraphTables(graphTables);
						pruningAlgorithm.setQueryTables(queryTables);
						
						pruningAlgorithm.setThreshold(0);
						pruningAlgorithm.compute();
						queryGraphMapping = pruningAlgorithm.getQueryGraphMapping();
						
						wcCandidatesNum += queryGraphMapping.get(startingNode).size();
						pruningAlgorithm.pathFilter(true);
						this.wcAfterNum += queryGraphMapping.get(startingNode).size();
//						pruningTime += watch.getElapsedTimeMillis();
						Multigraph prunedG = pruningAlgorithm.pruneGraph();
						watch.reset();
		
						List<RelatedQuery> relatedQueries;
		
						IsomorphicQuerySearch edAlgorithm = new IsomorphicQuerySearch();
						edAlgorithm.setStartingNode(startingNode);
						edAlgorithm.setLabelFreq(((BigMultigraph)G).getLabelFreq());
						edAlgorithm.setQuery(Q);
						edAlgorithm.setGraph(prunedG);
						edAlgorithm.setNumThreads(this.threadsNum);
						edAlgorithm.setQueryToGraphMap(pruningAlgorithm
								.getQueryGraphMapping());
						edAlgorithm.setLimitedComputation(true);
						edAlgorithm.compute();
						
						this.bfIntNum = Math.max(this.bfIntNum, IsomorphicQuerySearch.interNum);
						this.bfIntSum += IsomorphicQuerySearch.interNum;
						relatedQueries = edAlgorithm.getRelatedQueries();
						relatedQueriesUnique.addAll(relatedQueries);
						
		//				System.out.println(startingNode);
						
//						QuerySel qs = new QuerySel(G, wildCardQuery, startingNode);
//						
//						Cost.cost = 0;
		//				Cost.estimateMaxCost(wildCardQuery, startingNode, G, this.AVG_DEGREE, new HashSet<Edge>(), 1);
//						Cost.estimateMaxCostWithLabelMaxNum(wildCardQuery, startingNode, this.AVG_DEGREE, G, new HashSet<Edge>(), 1);
//		//				Cost.estimateExactCost(Q, startingNode, this.AVG_DEGREE, G, new HashSet<Edge>(), 1);
//		//				System.out.println(qs.getCanNumber(startingNode, this.AVG_DEGREE, 2));
//		//				double tt = qs.getCanNumber(startingNode, this.AVG_DEGREE, 2);
//		//				this.edCandidates += tt;
//		//				wcCost += tt* Cost.cost;
//		//				wcCost += qs.getCanNumber(startingNode, AVG_DEGREE, 2) * Cost.cost;
//		//				wcCost += Cost.getCandidatesNum(wildCardQuery, startingNode, G) * Cost.cost;
//						wcCost += qs.computeAdjNotCand(startingNode, AVG_DEGREE, this.neighbourNum) * Cost.cost;
//						this.wcAllEst += qs.getCanNumber(startingNode, this.AVG_DEGREE, this.neighbourNum);
//						this.wcAdjEdt += qs.computeAdjNotCand(startingNode, AVG_DEGREE, this.neighbourNum);
//						this.wcPathEst += qs.computePathNotCand(startingNode, AVG_DEGREE, this.neighbourNum);
		//				System.out.println(tt + " wc " + Cost.cost);
		//				wcCost += Cost.getCandidatesNum(wildCardQuery, startingNode, G) * Cost.cost;
		//				wcCost += Cost.estimateQueryCost(wildCardQuery, startingNode, G, AVG_DEGREE);
		//				System.out.println(wcCost + "  asd");
		//				comBw.newLine();
		//				answerNum += IsomorphicQuerySearch.answerCount;
		//				System.out.println(answerNum);
//						isoTime += watch.getElapsedTimeMillis();
						
					}
					
		//			comBw.close();
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
		//		System.out.println("asd:" + wcCost);
//				int queriesCount = 0;
		//		for (RelatedQuery related : relatedQueriesUnique) {
		//			queriesCount++;
		//		}
//				wcSearchCount = Utilities.searchCount / repititions;
//				wcBsCount /= repititions;
//				wcCmpCount /= repititions;
//				wcUptCount /= repititions;
//				wcSearchCount /= repititions;
		//		System.out.println("c:" + wcSearchCount);
		//		System.out.println(wcBsCount);
		//		System.out.println(wcCmpCount);
		//		System.out.println(wcUptCount);
		//		System.out.println(wcSearchCount);
				this.bfSearchCount = Utilities.searchCount / repititions;
			}
//			answerNum = relatedQueriesUnique.size();
//			System.out.println(this.wcIntNum + " " + this.wcIntSum);
		} else {
		
		}
		System.out.println(relatedQueriesUnique.size());
		System.out.println("search takes " + (System.nanoTime() - start));
		/*
		int ttCount = 0;
		Set<String> iso = new HashSet<>();
		Set<String> ed = new HashSet<>();
		Set<String> ed2 = new HashSet<>();
		int isoCount = 0;
		int ed1Count = 0;
		int ed2Count = 0;
		for (RelatedQuery rq : relatedQueriesUnique) {
//			if (ttCount > 100) {
//				break;
//			}
			List<Edge> edgeSet = new ArrayList<>();
			for (Entry<Edge, Edge> en: ((IsomorphicQuery)rq).getMappedEdges().entrySet()) {
				edgeSet.add(en.getValue());
//				System.out.println(en.getValue());
//				System.out.println(en.getValue().getSource() + " " + en.getValue().getLabel() + " " + en.getValue().getDestination());
//				System.out.println(con.toReadable(en.getValue()));
			}
			Map<Long, Integer> ansLabelCount = new HashMap<>();
			for (Edge e : edgeSet) {
				if (ansLabelCount.containsKey(e.getLabel())) {
					ansLabelCount.put(e.getLabel(), ansLabelCount.get(e.getLabel()) + 1);
				} else {
					ansLabelCount.put(e.getLabel(), 1);
				}
			}
			String tt = "";
//			String tt = con.toReadable(edgeSet);
			if (tt == null) {
				continue;
			}
			ttCount++;
			int distance = 0;
			for (Entry<Long, Integer> en : ansLabelCount.entrySet()) {
				if (!labelCount.containsKey(en.getKey()) || labelCount.get(en.getKey()) != en.getValue()) {
					distance++;
				}
			}
			if (distance == 0) {
//				System.out.println("iso");
//				iso.add(tt);
				isoCount++;
			} else if (distance == 1){
//				System.out.println("ed");
//				ed.add(tt);
				ed1Count++;
			} else {
//				ed2.add(tt);
				ed2Count++;
			}
//			System.out.println("count :" + count);
//			System.out.print(tt);
//			count++;
//			System.out.println("================");
		}
//		int count = 0;
//		System.out.println("Isomorphism:" + isoCount);
//		for (String mm : iso) {
//			System.out.println("count:" + count);
//			System.out.print(mm);
//			System.out.println("===============");
//			count++;
//		}
//		count = 0;
//		System.out.println("Edit distance 1:" + ed1Count);
//		System.out.println("Edit distance 2:" + ed2Count);
//		for (String mm : ed) {
//			System.out.println("count:" + count);
//			System.out.print(mm);
//			System.out.println("===============");
//			count++;
//		}
 * 
 */
		//to do
		this.bfElapsedTime = (double)(System.nanoTime() - startTime) / 1000000000.0;
	}
	public void runExtension() throws IOException, ParseException,
			AlgorithmExecutionException {
		long startTime = System.nanoTime();
		System.out.println("Running extension");
		Map<Long, Set<MappedNode>> queryGraphMapping = null;
		ComputeGraphNeighbors tableAlgorithm = null;
		NeighborTables queryTables = null;
		NeighborTables graphTables = null;
		PruningAlgorithm pruningAlgorithm = null;
//		NextQueryVertexes nqv = null;
//		Isomorphism iso = null;
//		float loadingTime = 0;
//		float computingNeighborTime = 0;
//		float pruningTime = 0;
//		float isoTime = 0;
		this.exBsCount = 0;
		this.exCmpCount = 0;
		this.exUptCount = 0;
		this.exSearchCount = 0;
		Utilities.searchCount = 0;
		String comFile = "comparison.txt";
//		BufferedWriter comBw = new BufferedWriter(new FileWriter(comFile, true));
		Long startingNode;
		Indexing ind = new Indexing();
		Selectivity sel = new Selectivity();
		ArrayList<InfoNode> infoNodes = new ArrayList<>();
		this.exCandidatesNum = 0;
		this.exAfterNum = 0;
		HashSet<EditDistanceQuery> relatedQueriesUnique = new HashSet<>();
		this.isEdBad = false;
		this.edIntNum = 0;
		for (int exprimentTime = 0; exprimentTime < repititions; exprimentTime++) {
			StopWatch watch = new StopWatch();
			watch.start();

			Q = new BigMultigraph(queryName, queryName, true);
			
//			ind.indexing((BigMultigraph) G);

//			if (!this.isQueryMappable(Q)) {
//				break;
//			} 
//			loadingTime += watch.getElapsedTimeMillis();

			tableAlgorithm = new ComputeGraphNeighbors();
			watch.reset();
			tableAlgorithm.setNumThreads(threadsNum);
			tableAlgorithm.setK(neighbourNum);
			/**
			tableAlgorithm.setGraph(G);
			tableAlgorithm.compute();
			tableAlgorithm.computePathFilter();
			**/
			graphTables = gTableAlgorithm.getNeighborTables();
			tableAlgorithm.setGraph(Q);
			tableAlgorithm.compute();
			queryTables = tableAlgorithm.getNeighborTables();
//			computingNeighborTime += watch.getElapsedTimeMillis();
			watch.reset();
//			startingNode = this.getRootNode(true);
			startingNode = this.getRootNode(true);
//			InfoNode info = new InfoNode(startingNode);
			// System.out.println("starting node:" + startingNode);
			pruningAlgorithm = new PruningAlgorithm();
			// Set starting node according to sels of nodes.
			pruningAlgorithm.setStartingNode(startingNode);
			pruningAlgorithm.setGraph(G);
			pruningAlgorithm.setQuery(Q);
			pruningAlgorithm.setK(this.neighbourNum);
			pruningAlgorithm.setGraphTables(graphTables);
			pruningAlgorithm.setQueryTables(queryTables);
			pruningAlgorithm.setThreshold(this.threshold);
			pruningAlgorithm.setgPathTables(gTableAlgorithm.getPathTables());
			pruningAlgorithm.compute();
			
			
//			this.exBsCount = pruningAlgorithm.getBsCount();
//			this.exCmpCount = pruningAlgorithm.getCmpNbLabel();
//			this.exUptCount = pruningAlgorithm.getUptCount();
//			info.setBsCount(pruningAlgorithm.getBsCount());
//			info.setCmpCount(pruningAlgorithm.getCmpNbLabel());
//			info.setUptCount(pruningAlgorithm.getUptCount());
//			infoNodes.add(info);
			queryGraphMapping = pruningAlgorithm.getQueryGraphMapping();
//			this.exCandidatesNum = queryGraphMapping.get(startingNode).size();
//			System.out.println("ex before:" + exCandidatesNum);
//			pruningAlgorithm.pathFilter();
//			this.exAfterNum = queryGraphMapping.get(startingNode).size();
//			this.exPathOnly = pruningAlgorithm.onlyPath();
//			System.out.println("ex after:" + queryGraphMapping.get(startingNode).size());
//			pruningTime += watch.getElapsedTimeMillis();
			watch.reset();

			List<RelatedQuery> relatedQueries;

			EditDistanceQuerySearch edAlgorithm = new EditDistanceQuerySearch();
			edAlgorithm.setStartingNode(startingNode);
			
			edAlgorithm.setQuery(Q);
			edAlgorithm.setGraph(G);
			edAlgorithm.setNumThreads(this.threadsNum);
			edAlgorithm.setQueryToGraphMap(pruningAlgorithm
					.getQueryGraphMapping());
			edAlgorithm.setLimitedComputation(false);
			edAlgorithm.setThreshold(threshold);
			edAlgorithm.compute();
//			this.isEdBad = EditDistanceQuerySearch.isBad;
			this.edIntNum = Math.max(this.edIntNum, EditDistanceQuerySearch.interNum);
//			relatedQueries = edAlgorithm.getRelatedQueries();
//			relatedQueriesUnique.addAll(relatedQueries);
//			System.out.println("intermediate:" + this.edIntNum);
			
			QuerySel qs = new QuerySel(G, Q, startingNode);
//			adj = qs.computeSelAdjNotCorrelated(1, startingNode, new HashSet<>(), 0, this.neighbourNum);
//			path = qs.computeSelPathNotCorrelated(1, startingNode, new HashSet<>(), 0, this.neighbourNum);
//			all = qs.computeSelAllNotCorrelated(Q);
//			this.edCandidates -=  (Q.edgeSet().size() - 1) * qs.getCanNumber(startingNode, this.AVG_DEGREE, 2);
			
//			System.out.println(G.vertexSet().size()*qs.edProb(startingNode, this.AVG_DEGREE, 2));
//			System.out.println(qs.getEdCanNumber(startingNode, AVG_DEGREE, 2));
			Cost.cost = 0;
//			Cost.estimateMaxCost(Q, startingNode, G, this.AVG_DEGREE, new HashSet<Edge>(), 1);
//			Cost.estimateMaxCostWithLabelMaxNum(Q, startingNode, this.AVG_DEGREE, G, new HashSet<Edge>(), 1);
			Cost.estimateEdExactCost(Q, startingNode, AVG_DEGREE, G, new ArrayList<Edge>());
//			System.out.println(Cost.cost);
//			double tt = qs.getEdCanNumber(startingNode, AVG_DEGREE, 2);
//			edCost = tt  * Cost.cost;
//			edCost = qs.getEdCanNumber(startingNode, AVG_DEGREE, 2) * Cost.cost;
//			edCost = Cost.getCandidatesNum(Q, startingNode, G) * Cost.cost;
//			edCost = qs.computeAdjNotCand(startingNode, AVG_DEGREE, this.neighbourNum) * Cost.cost;
			this.edAllEst = qs.getEdCanNumber(startingNode, AVG_DEGREE, this.neighbourNum) * Cost.cost;
			this.edAdjEdt = qs.computeAdjNotCand(startingNode, AVG_DEGREE, this.neighbourNum) * Cost.cost;
			this.edPathEst = qs.computePathNotCand(startingNode, AVG_DEGREE, this.neighbourNum) * Cost.cost;
//			System.out.println(startingNode + " " + tt + " ed  " + Cost.cost);
//			System.out.println(startingNode);
//			System.out.println("eds:" +this.edCandidates);
			String[] temp = queryName.split("/");
			
//			if (relatedQueriesUnique.size() != 0) {
//				
//				System.out.println(temp[temp.length - 1]);
//			}
//			int tempCount = count;
//			for (RelatedQuery rq : relatedQueriesUnique) {
//				if (tempCount - count >= 1) break;
//				BufferedWriter bw = new BufferedWriter(new FileWriter("./queryFolder/AOL_Sim/" + temp[temp.length - 1]));
//				for (Edge e : rq.getEdgeSet()) {
//					bw.write(e.getSource() + " " + e.getDestination() + " " + e.getLabel());
//					bw.newLine();
//				}
//				bw.close();
//				tempCount++;
//			}
//			count = tempCount;
//			comBw.newLine();
//			isoTime += watch.getElapsedTimeMillis();
			
		}

//		comBw.close();
//		Q = null;
//		loadingTime = loadingTime / repititions;
//		computingNeighborTime = computingNeighborTime / repititions;
//		pruningTime = pruningTime / repititions;
//		isoTime = isoTime / repititions;
//		exSearchCount = Utilities.searchCount;
//		exBsCount = exBsCount / repititions;
//		exCmpCount = exCmpCount / repititions;
//		exUptCount = exUptCount / repititions;
		exSearchCount = Utilities.searchCount / repititions;
		exElapsedTime = (double)(System.nanoTime() - startTime) / 1000000000;
//		System.out.println(exBsCount);
//		System.out.println(exCmpCount);
//		System.out.println(exUptCount);
//		System.out.println(exSearchCount);
	}
	
	public void runBFExtension() throws IOException, ParseException,
	AlgorithmExecutionException {
		long startTime = System.nanoTime();
		System.out.println("Running extension");
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
		this.exBFTime = 0;
		this.exBFSearchCount = 0;
		Utilities.searchCount = 0;
//		String comFile = "comparison.txt";
		//BufferedWriter comBw = new BufferedWriter(new FileWriter(comFile, true));
		Long startingNode;
//		Indexing ind = new Indexing();
//		Selectivity sel = new Selectivity();
//		ArrayList<InfoNode> infoNodes = new ArrayList<>();
//		this.exCandidatesNum = 0;
//		this.exAfterNum = 0;
		HashSet<EditDistanceQuery> relatedQueriesUnique = new HashSet<>();
		EditDistanceQuerySearch.answerCount = 0;
		this.isEdBad = false;
		this.edIntNum = 0;
		for (int exprimentTime = 0; exprimentTime < repititions; exprimentTime++) {
			StopWatch watch = new StopWatch();
			watch.start();
		
			Q = new BigMultigraph(queryName, queryName, true);
			
		//	ind.indexing((BigMultigraph) G);
		
		//	if (!this.isQueryMappable(Q)) {
		//		break;
		//	} 
		//	loadingTime += watch.getElapsedTimeMillis();
		
			tableAlgorithm = new ComputeGraphNeighbors();
			watch.reset();
			tableAlgorithm.setNumThreads(threadsNum);
			tableAlgorithm.setK(neighbourNum);
			/**
			tableAlgorithm.setGraph(G);
			tableAlgorithm.compute();
			tableAlgorithm.computePathFilter();
			**/
			graphTables = gTableAlgorithm.getNeighborTables();
			tableAlgorithm.setGraph(Q);
			tableAlgorithm.compute();
			queryTables = tableAlgorithm.getNeighborTables();
		//	computingNeighborTime += watch.getElapsedTimeMillis();
			watch.reset();
		//	startingNode = this.getRootNode(true);
			startingNode = this.getRootNode(true);
		//	InfoNode info = new InfoNode(startingNode);
			// System.out.println("starting node:" + startingNode);
			pruningAlgorithm = new PruningAlgorithm();
			// Set starting node according to sels of nodes.
			pruningAlgorithm.setStartingNode(startingNode);
			pruningAlgorithm.setGraph(G);
			pruningAlgorithm.setQuery(Q);
			pruningAlgorithm.setK(this.neighbourNum);
			pruningAlgorithm.setGraphTables(graphTables);
			pruningAlgorithm.setQueryTables(queryTables);
			pruningAlgorithm.setThreshold(this.threshold);
			pruningAlgorithm.setgPathTables(gTableAlgorithm.getPathTables());
			pruningAlgorithm.compute();
			
			
		//	this.exBsCount = pruningAlgorithm.getBsCount();
		//	this.exCmpCount = pruningAlgorithm.getCmpNbLabel();
		//	this.exUptCount = pruningAlgorithm.getUptCount();
		//	info.setBsCount(pruningAlgorithm.getBsCount());
		//	info.setCmpCount(pruningAlgorithm.getCmpNbLabel());
		//	info.setUptCount(pruningAlgorithm.getUptCount());
		//	infoNodes.add(info);
//			queryGraphMapping = pruningAlgorithm.getQueryGraphMapping();
//			this.exCandidatesNum = queryGraphMapping.get(startingNode).size();
		//	System.out.println("ex before:" + exCandidatesNum);
			pruningAlgorithm.pathFilter();
//			this.exAfterNum = queryGraphMapping.get(startingNode).size();
//			this.exPathOnly = pruningAlgorithm.onlyPath();
		//	System.out.println("ex after:" + queryGraphMapping.get(startingNode).size());
		//	pruningTime += watch.getElapsedTimeMillis();
			watch.reset();
		
			List<RelatedQuery> relatedQueries;
		
			EditDistanceQuerySearch edAlgorithm = new EditDistanceQuerySearch();
			edAlgorithm.setStartingNode(startingNode);
			
			edAlgorithm.setQuery(Q);
			edAlgorithm.setGraph(G);
			edAlgorithm.setNumThreads(this.threadsNum);
			edAlgorithm.setQueryToGraphMap(pruningAlgorithm
					.getQueryGraphMapping());
			edAlgorithm.setLimitedComputation(false);
			edAlgorithm.setThreshold(threshold);
			edAlgorithm.compute();
		//	this.isEdBad = EditDistanceQuerySearch.isBad;
//			this.edIntNum = Math.max(this.edIntNum, EditDistanceQuerySearch.interNum);
			relatedQueries = edAlgorithm.getRelatedQueries();
			System.out.println(relatedQueries.size());
			for (RelatedQuery rq : relatedQueries) {
				relatedQueriesUnique.add((EditDistanceQuery)rq);
			}
//			relatedQueriesUnique.addAll(relatedQueries);
			System.out.println(relatedQueriesUnique.size());
			int count = 0;
			for (RelatedQuery rq : relatedQueriesUnique) {
				System.out.println("count :" + count);
				for (Edge e : rq.getEdgeSet()) {
					System.out.println(e);
				}
				count++;
				System.out.println("================");
			}
		//	System.out.println("intermediate:" + this.edIntNum);
			/**
			QuerySel qs = new QuerySel(G, Q, startingNode);
		//	adj = qs.computeSelAdjNotCorrelated(1, startingNode, new HashSet<>(), 0, this.neighbourNum);
		//	path = qs.computeSelPathNotCorrelated(1, startingNode, new HashSet<>(), 0, this.neighbourNum);
		//	all = qs.computeSelAllNotCorrelated(Q);
		//	this.edCandidates -=  (Q.edgeSet().size() - 1) * qs.getCanNumber(startingNode, this.AVG_DEGREE, 2);
			
		//	System.out.println(G.vertexSet().size()*qs.edProb(startingNode, this.AVG_DEGREE, 2));
		//	System.out.println(qs.getEdCanNumber(startingNode, AVG_DEGREE, 2));
			Cost.cost = 0;
		//	Cost.estimateMaxCost(Q, startingNode, G, this.AVG_DEGREE, new HashSet<Edge>(), 1);
		//	Cost.estimateMaxCostWithLabelMaxNum(Q, startingNode, this.AVG_DEGREE, G, new HashSet<Edge>(), 1);
			Cost.estimateEdExactCost(Q, startingNode, AVG_DEGREE, G, new ArrayList<Edge>());
		//	System.out.println(Cost.cost);
		//	double tt = qs.getEdCanNumber(startingNode, AVG_DEGREE, 2);
		//	edCost = tt  * Cost.cost;
		//	edCost = qs.getEdCanNumber(startingNode, AVG_DEGREE, 2) * Cost.cost;
		//	edCost = Cost.getCandidatesNum(Q, startingNode, G) * Cost.cost;
		//	edCost = qs.computeAdjNotCand(startingNode, AVG_DEGREE, this.neighbourNum) * Cost.cost;
			this.edAllEst = qs.getEdCanNumber(startingNode, AVG_DEGREE, this.neighbourNum) * Cost.cost;
			this.edAdjEdt = qs.computeAdjNotCand(startingNode, AVG_DEGREE, this.neighbourNum) * Cost.cost;
			this.edPathEst = qs.computePathNotCand(startingNode, AVG_DEGREE, this.neighbourNum) * Cost.cost;
		//	System.out.println(startingNode + " " + tt + " ed  " + Cost.cost);
		//	System.out.println(startingNode);
		//	System.out.println("eds:" +this.edCandidates);
			String[] temp = queryName.split("/");
			**/
		//	if (relatedQueriesUnique.size() != 0) {
		//		
		//		System.out.println(temp[temp.length - 1]);
		//	}
		//	int tempCount = count;
		//	for (RelatedQuery rq : relatedQueriesUnique) {
		//		if (tempCount - count >= 1) break;
		//		BufferedWriter bw = new BufferedWriter(new FileWriter("./queryFolder/AOL_Sim/" + temp[temp.length - 1]));
		//		for (Edge e : rq.getEdgeSet()) {
		//			bw.write(e.getSource() + " " + e.getDestination() + " " + e.getLabel());
		//			bw.newLine();
		//		}
		//		bw.close();
		//		tempCount++;
		//	}
		//	count = tempCount;
		//	comBw.newLine();
		//	isoTime += watch.getElapsedTimeMillis();
			
		}
		
		//comBw.close();
		//Q = null;
		//loadingTime = loadingTime / repititions;
		//computingNeighborTime = computingNeighborTime / repititions;
		//pruningTime = pruningTime / repititions;
		//isoTime = isoTime / repititions;
		//exSearchCount = Utilities.searchCount;
		//exBsCount = exBsCount / repititions;
		//exCmpCount = exCmpCount / repititions;
		//exUptCount = exUptCount / repititions;
		this.exBFSearchCount = Utilities.searchCount / repititions;
		this.exBFTime = (double)(System.nanoTime() - startTime) / 1000000000;
		this.answerNum = EditDistanceQuerySearch.answerCount;
		//System.out.println(exBsCount);
		//System.out.println(exCmpCount);
		//System.out.println(exUptCount);
		//System.out.println(exSearchCount);
	}
	public void runEditDistance() throws AlgorithmExecutionException,
			ParseException, IOException {
		String[] temp = queryName.split("/");
		System.out.println(temp[temp.length - 1]);
		
		
//		long degrees = 0;
//		for (Entry<Long, Long> nodes: ((BigMultigraph)G).getNodeDegree().entrySet()){
//			degrees += nodes.getValue();
//		}
//		double averageDegree = degrees / (double) ((BigMultigraph)G).getNodeDegree().entrySet().size();
//		System.out.println(averageDegree);
		switch (mo) {
		case WILD_CARD:
			this.runWildCard();
			break;
		case EXTENSION:
			this.runExtension();
			break;
		case BOTH:
			this.runWildCard();
//			this.runBFWildCard();
//			this.runExtension();
//			this.runBFExtension();
			break;
		default:
			throw new IllegalArgumentException("Wrong running arguements");
		}
		StringBuilder sb = new StringBuilder();
		long maxDegree = ((BigMultigraph)G).getMaxDegree();
		double wcEstimatedNum = 0;
		double exEstimatedNum = 0;
		double wcEstimatedCost = 0;
		double exEstimatedCost = 0;
		double selSum = 0;
//		List<Edge> sortedEdges = this.sortEdge(Q.edgeSet());
//		System.out.println("max:" + ((BigMultigraph)G).getMaxDegree());
//		double a = ((BigMultigraph)G).getLabelFreq().get(sortedEdges.get(0).getLabel()).getFrequency()/((double)G.edgeSet().size());
//		double b = ((BigMultigraph)G).getLabelFreq().get(sortedEdges.get(1).getLabel()).getFrequency()/((double)G.edgeSet().size());
//		double c =  ((BigMultigraph)G).getLabelFreq().get(sortedEdges.get(2).getLabel()).getFrequency()/((double)G.edgeSet().size());
//		double d = ((BigMultigraph)G).getLabelFreq().get(sortedEdges.get(3).getLabel()).getFrequency()/((double)G.edgeSet().size());
//		double e = ((BigMultigraph)G).getLabelFreq().get(sortedEdges.get(4).getLabel()).getFrequency()/((double)G.edgeSet().size());
//		wcEstimatedNum = G.vertexSet().size() * Utilities.choose(this.AVG_DEGREE, 4) * (a*b*c*d + a*b*c*e + a*c*d*e + a*b*d*e + b*c*d*e);
//		exEstimatedNum = G.vertexSet().size() * Utilities.choose(this.AVG_DEGREE, 4) * (a*b*c*d*e + (1-a)*b*c*d*e + a*(1-b)*c*d*e + a*b*(1-c)*d*e + a*b*c*(1-d))*e + a*b*c*d*(1-e);
//		sb.append(temp[temp.length - 1] + "," + this.wcCandidatesNum + "," + wcEstimatedNum + ", " + this.exCandidatesNum + "," + exEstimatedNum);
//		wcEstimatedCost = G.vertexSet().size() * Utilities.choose(this.AVG_DEGREE, 2) * a*b*(this.AVG_DEGREE * a + Math.pow(this.AVG_DEGREE, 2) * a *b + Math.pow(this.AVG_DEGREE, 3) * a *b);
//		wcEstimatedCost += G.vertexSet().size() * Utilities.choose(this.AVG_DEGREE, 2) * a*c*(this.AVG_DEGREE * a + Math.pow(this.AVG_DEGREE, 2) * a *c + Math.pow(this.AVG_DEGREE, 3) * a *c);
//		wcEstimatedCost += G.vertexSet().size() * Utilities.choose(this.AVG_DEGREE, 2) * c*b*(this.AVG_DEGREE * b + Math.pow(this.AVG_DEGREE, 2) * b *c + Math.pow(this.AVG_DEGREE, 3) * b *c);
//		exEstimatedCost = exEstimatedNum * (this.AVG_DEGREE + this.AVG_DEGREE * this.AVG_DEGREE + 
//				Utilities.choose(this.AVG_DEGREE, 2)* (a+b) * this.AVG_DEGREE);
//		sb.append("," + this.wcSearchCount + "," + wcEstimatedCost + "," + this.exSearchCount + "," + exEstimatedCost);
//		sb.append(temp[temp.length - 1] + ","  + this.wcSearchCount + ","  + this.exSearchCount + "," + this.wcCandidatesNum + ","  + exCandidatesNum + ","+ answerNum + "," + this.wcElapsedTime + "," + this.exElapsedTime + "," + (this.isWcBad ? 1 : 0) + "," + (this.isEdBad ? 1 : 0) + "," + this.wcIntNum + "," + this.wcIntSum + "," + this.edIntNum);
		sb.append(temp[temp.length - 1] + "," + this.wcSearchCount + ","  + this.exSearchCount + "," + this.bfSearchCount + ","  + this.exBFSearchCount+ "," + this.wcElapsedTime + "," + this.exElapsedTime + "," + this.bfElapsedTime + "," + this.exBFTime + ","+ this.wcIntNum +"," + this.wcIntSum + "," + this.edIntNum + "," + this.bfIntNum + "," + this.bfIntSum + "," + this.answerNum);
		//		System.out.println(answerNum);
//		System.out.println("size:" + G.edgeSet().size());
//		if (this.exPathOnly > 10) {
//			this.appendCand(this.candComp, temp[temp.length - 1] , this.wcCandidatesNum, this.wcAfterNum, this.wcPathOnly, this.exCandidatesNum, this.exAfterNum, this.exPathOnly);
//		}
//		this.appendSels(this.selsComp, temp[temp.length - 1] , this.wcCandidatesNum, this.wcAllEst, this.wcAdjEdt, this.wcPathEst, this.exCandidatesNum, this.edAllEst, this.edAdjEdt, this.edPathEst);
//		this.append(strList, temp[temp.length - 1], this.wcAllEst, this.wcAdjEdt, this.wcPathEst, this.edAllEst, this.edAdjEdt, this.edPathEst);
		/*for (Edge e : Q.edgeSet()) {
			double sel = ((BigMultigraph)G).getLabelFreq().get(e.getLabel()).getFrequency()/((double)G.edgeSet().size());
			selSum += sel; 
			wcEstimatedNum += G.vertexSet().size() * (1 - Math.pow(1 - sel, this.AVG_DEGREE));
			wcEstimatedCost += G.vertexSet().size() * (1 - Math.pow(1 - sel, maxDegree)) * (this.AVG_DEGREE * sel + this.AVG_DEGREE * this.AVG_DEGREE * sel);
			System.out.println("wc candidate:" + G.vertexSet().size() * (1 - Math.pow(1 - sel, this.AVG_DEGREE)));
			System.out.println("wc mul:" + (this.AVG_DEGREE * sel + this.AVG_DEGREE * this.AVG_DEGREE * sel));
		}
		List<Edge> sortedEdges = this.sortEdge(Q.edgeSet());
		exEstimatedNum = G.vertexSet().size() * (1 - Math.pow(1 - selSum, this.AVG_DEGREE));
		double a = ((BigMultigraph)G).getLabelFreq().get(sortedEdges.get(0).getLabel()).getFrequency()/((double)G.edgeSet().size());
		double b = ((BigMultigraph)G).getLabelFreq().get(sortedEdges.get(1).getLabel()).getFrequency()/((double)G.edgeSet().size());
		exEstimatedCost = exEstimatedNum * (this.AVG_DEGREE + this.AVG_DEGREE * (this.AVG_DEGREE - 1) * a + 
				this.AVG_DEGREE * (this.AVG_DEGREE - 1) * (1 - a) * b);
		System.out.println("ex candidate:" + exEstimatedNum);
		System.out.println("ex mul:" + (this.AVG_DEGREE + this.AVG_DEGREE * (this.AVG_DEGREE - 1) * a + 
				this.AVG_DEGREE * (this.AVG_DEGREE - 1) * (1 - a) * b));
//		sb.append(" , " + temp[temp.length - 1] + "," + this.wcSearchCount + "," +  this.exSearchCount + "," + (this.exSearchCount - this.wcSearchCount));
		sb.append(temp[temp.length - 1] + "," + this.wcCandidatesNum + "," + wcEstimatedNum + ", " + this.exCandidatesNum + "," + exEstimatedNum + ",");
		sb.append(this.wcSearchCount + "," + wcEstimatedCost + "," + this.exSearchCount + "," + exEstimatedCost);
		System.out.println(this.wcBsCount + " " + this.exBsCount);
		System.out.println(this.wcCmpCount + " " + this.exCmpCount);
		System.out.println(this.wcUptCount + " " + this.exUptCount);
		System.out.println(this.wcSearchCount + " " + this.exSearchCount);
//		if (this.wcSearchCount < this.exSearchCount) {
//			sb.append(",wc better");
//			System.out.println("wc better");
//		} else {
//			sb.append(",ex better");
//			System.out.println("ex better");
//		}
		sb.append("," + wcElapsedTime + "," + exElapsedTime);
//		for (Edge e : Q.edgeSet()) {
//			sb.append("," + ((BigMultigraph)G).getLabelFreq().get(e.getLabel()).getFrequency()/((double)G.edgeSet().size()));
//		}
		System.out.println(a + " " + b + " " + sb.toString());
//		*/
		cmpBw.write(sb.toString());
		cmpBw.newLine();
		cmpBw.flush();
	}
	
	public void write(String fileName) {
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
//			bw.write("avg degree: 8.97, wc candidates, wc estimated candidates, ex candidates, ex estimated candidates, answer count, wc time, ex time, all, adj, path");
			bw.write("avg degree: 8.97, wc cost, ed cost, wc candidate, ed candidate, answer count, wc time, ex time, isWcBad, isEdBad, wcIntNum, wcIntSum, edIntNum");
			bw.newLine();
			for (String str : strList) {
				if (str.length() == 0 || str.startsWith("avg degree")) {
					continue;
				}
				System.out.println(str);
				bw.write(str);
				bw.newLine();
				bw.flush();
			}
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void writeCand(String fileName) {
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(fileName, true));
			bw.write(" , wc, wc path, wc after, ed, ed path, ed after");
			bw.newLine();
			for (String str : this.candComp) {
				if (str.length() == 0) {
					continue;
				}
				bw.write(str);
				bw.newLine();
				bw.flush();
			}
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void writeSels(String fileName) {
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
			bw.write(" , wc, wcAllEst, wcAdj, wcPath, ed, edAllest, edAdj, edPath");
			bw.newLine();
			for (String str : this.selsComp) {
				if (str.length() == 0) {
					continue;
				}
				System.out.println(str);
				bw.write(str);
				bw.newLine();
				bw.flush();
			}
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void appendCand(List<String> strList, String fileName, int wc, int wcAfter, int wcPath, int ex, int exAfter, int exPath) {
		String goal = fileName + "," + wc +  "," + wcPath + "," + wcAfter +"," + ex + ","  + exPath+ "," + exAfter;
		strList.add(goal);
	}
	
	public void appendSels(List<String> strList, String fileName, int wc, double wcAllEst, double wcAdjEst, double wcPathEst, int ed, double edAllEst, double edAdjEst, double edPathEst) {
		String goal = fileName + "," + wc +  "," + wcAllEst + "," + wcAdjEst +"," + wcPathEst + "," + ed + "," + edAllEst + "," + edAdjEst +"," + edPathEst;
		strList.add(goal);
	}
	
	public void append(List<String> strList, String fileName, double all, double adj, double path) {
		String goal = null;
		for (String str : strList) {
			if (str.startsWith(fileName)) {
				goal = str;
				break;
			}
		}
		strList.remove(goal);
		if (goal != null) {
		goal += "," + all + "," + adj + "," + path;
		strList.add(goal);
		}
	}
	
	public void append(List<String> strList, String fileName, double wcAll, double wcAdj, double wcPath, double edAll, double edAdj, double edPath) {
		String goal = null;
		for (String str : strList) {
			if (str.startsWith(fileName)) {
				goal = str;
				break;
			}
		}
		strList.remove(goal);
		if (goal != null) {
		goal += "," + wcAll + "," + wcAdj + "," + wcPath +  "," + edAll + "," + edAdj + "," + edPath;
		strList.add(goal);
		}
	}
	
	public void append(List<String> strList, String fileName, double sel) {
		String goal = null;
		for (String str : strList) {
			if (str.startsWith(fileName)) {
				goal = str;
				break;
			}
		}
		strList.remove(goal);
		if (goal != null) {
		goal += "," + sel;
		strList.add(goal);
		}
	}
	public void append(List<String> strList, String fileName, double wcCost, double edCost) {
		String goal = null;
		for (String str : strList) {
			if (str.startsWith(fileName)) {
				goal = str;
				break;
			}
		}
		strList.remove(goal);
		if (goal != null) {
		goal += "," + wcCost + "," + edCost;
		strList.add(goal);
		}
	}
	
	public List<String> readFile(String fileName) {
		List<String> strList = new ArrayList<String>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(fileName));
			String str = null;
			while((str = br.readLine()) != null) {
					strList.add(str);
			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return strList;
	}
	
	public void findMaxEstimation() {
		
	}
	
	public Long getRootNode() {
		Collection<Long> nodes = this.Q.vertexSet();
		for (Long node : nodes) {
			int degree = Q.inDegreeOf(node) + Q.outDegreeOf(node);
			if (degree == 1) {
				return node;
			}
		}
		return nodes.iterator().next();
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

        for( Edge e : this.Q.edgeSet()){
            bestLabel = e.getLabel() > bestLabel ? e.getLabel() : bestLabel;
        }

//        if(minimumFrquency) {
//         bestLabel =this.findLessFrequentLabel(edgeLabels);
//        } else {
//         bestLabel =this.findMostFrequentLabel(edgeLabels);
//        }

        if(bestLabel == null || bestLabel == -1L ){
            throw new AlgorithmExecutionException("Best Label not found when looking for a root node!");
        }


        Collection<Edge> edgesIn, edgesOut;

        for (Long concept : nodes) {
            tempFreq = this.Q.inDegreeOf(concept)+ this.Q.outDegreeOf(concept);

            edgesIn = this.Q.incomingEdgesOf(concept);

            for (Edge Edge : edgesIn) {
                if (Edge.getLabel().equals(bestLabel)) {
                    if(tempFreq > maxFreq){
                        goodNode = concept;
                        maxFreq = tempFreq;
                    }
                }
            }

            edgesOut = this.Q.outgoingEdgesOf(concept);
            for (Edge Edge : edgesOut) {
                if (Edge.getLabel().equals(bestLabel)) {
                    if(tempFreq > maxFreq){
                        goodNode = concept;
                        maxFreq = tempFreq;
                    }
                }
            }
        }

        return goodNode;
    }
	public MethodOption getMo() {
		return mo;
	}

	public void setMo(MethodOption mo) {
		this.mo = mo;
	}

	protected boolean isQueryMappable(Multigraph queryGraph) {

		for (Long node : queryGraph.vertexSet()) {
			if (queryGraph.outDegreeOf(node) > G.outDegreeOf(node)) {
				System.out.println(queryName + " is not mappable");
				return false;
			}

			Collection<Edge> edges = G.outgoingEdgesOf(node);

			for (Edge e : queryGraph.outgoingEdgesOf(node)) {
				if (e.getLabel().equals(0L)) {
					continue;
				}
				if (!edges.contains(e)) {
					System.out.println(queryName + " is not mappable");
					return false;
				}
			}
		}
		return true;
	}

	public int getRepititions() {
		return repititions;
	}

	public void setRepititions(int repititions) {
		this.repititions = repititions;
	}

	public int getThreshold() {
		return threshold;
	}

	public void setThreshold(int threshold) {
		this.threshold = threshold;
	}

	public int getThreadsNum() {
		return threadsNum;
	}

	public void setThreadsNum(int threadsNum) {
		this.threadsNum = threadsNum;
	}

	public int getNeighbourNum() {
		return neighbourNum;
	}

	public void setNeighbourNum(int neighbourNum) {
		this.neighbourNum = neighbourNum;
	}

	public String getGraphName() {
		return graphName;
	}

	public List<String> getSelsComp() {
		return selsComp;
	}

	public void setSelsComp(List<String> selsComp) {
		this.selsComp = selsComp;
	}

	public void setGraphName(String graphName) {
		this.graphName = graphName;
	}

	public String getQueryName() {
		return queryName;
	}

	public void setQueryName(String queryName) {
		this.queryName = queryName;
	}

	public String getOutputFile() {
		return outputFile;
	}

	public void setOutputFile(String outputFile) {
		this.outputFile = outputFile;
	}

	public String getAnswerFile() {
		return answerFile;
	}

	public void setAnswerFile(String answerFile) {
		this.answerFile = answerFile;
	}

	public Multigraph getQ() {
		return Q;
	}

	public void setQ(Multigraph q) {
		Q = q;
	}

	public HashSet<RelatedQuery> getRelatedQueriesUnique() {
		return relatedQueriesUnique;
	}

	public void setRelatedQueriesUnique(
			HashSet<RelatedQuery> relatedQueriesUnique) {
		this.relatedQueriesUnique = relatedQueriesUnique;
	}
	
	public List<Edge> sortEdge(Collection<Edge> edges) {
		if (edges == null) {
			return null;
		}
		List<Edge> sortedEdges = new ArrayList<>();
    	PriorityQueue<Edge> pq = new PriorityQueue<>( new Comparator<Edge>(){
    		public int compare(Edge e1, Edge e2) {
    			if (e1.getLabel().equals(0L)) {
    				return 1;
    			} else if (e2.getLabel().equals(0L)) {
    				return -1;
    			} else {
    				LabelContainer lc1 = ((BigMultigraph)G).getLabelFreq().get(e1.getLabel());
    				LabelContainer lc2 = ((BigMultigraph)G).getLabelFreq().get(e2.getLabel());
    				if (lc1 == null) {
    					return 1;
    				}
    				
    				if (lc2 == null) {
    					return -1;
    				}
    				return (int)(lc1.getFrequency() - lc2.getFrequency());
    			}
    		}
    	});
    	for (Edge qe : edges) {
    		pq.add(qe);
    	}
    	while(!pq.isEmpty()) {
    		sortedEdges.add(pq.poll());
    	}
    	return sortedEdges;
	}

	public List<String> getStrList() {
		return strList;
	}

	public void setStrList(List<String> strList) {
		this.strList = strList;
	}

	public Multigraph getG() {
		return G;
	}

	public void setG(Multigraph g) {
		G = g;
	}

	public List<String> getCandComp() {
		return candComp;
	}

	public void setCandComp(List<String> candComp) {
		this.candComp = candComp;
	}

	public ComputeGraphNeighbors getgTableAlgorithm() {
		return gTableAlgorithm;
	}

	public void setgTableAlgorithm(ComputeGraphNeighbors gTableAlgorithm) {
		this.gTableAlgorithm = gTableAlgorithm;
	}
	

}
