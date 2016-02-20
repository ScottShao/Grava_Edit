package eu.unitn.disi.db.grava.scc;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.exemplar.core.RelatedQuery;
import eu.unitn.disi.db.exemplar.core.algorithms.ComputeGraphNeighbors;
import eu.unitn.disi.db.exemplar.core.algorithms.ComputePathGraphNeighbors;
import eu.unitn.disi.db.exemplar.core.algorithms.EditDistanceQuerySearch;
import eu.unitn.disi.db.exemplar.core.algorithms.IsomorphicQuerySearch;
import eu.unitn.disi.db.exemplar.core.algorithms.PruningAlgorithm;
import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Indexing;
import eu.unitn.disi.db.grava.graphs.InfoNode;
import eu.unitn.disi.db.grava.graphs.LabelContainer;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.graphs.Selectivity;
import eu.unitn.disi.db.grava.utils.FileOperator;
import eu.unitn.disi.db.grava.utils.NodeCostComparator;
import eu.unitn.disi.db.grava.utils.NodeExSelComparator;
import eu.unitn.disi.db.grava.utils.NodeSelComparator;
import eu.unitn.disi.db.grava.vectorization.NeighborTables;
import eu.unitn.disi.db.grava.vectorization.PathNeighborTables;
import eu.unitn.disi.db.grava.graphs.MappedNode;
import eu.unitn.disi.db.mutilities.StopWatch;

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

	public void runEditDistance() throws ParseException, IOException,
			AlgorithmExecutionException {

		Map<Long, Set<MappedNode>> queryGraphMapping = null;
		ComputeGraphNeighbors tableAlgorithm = null;
		// ComputePathGraphNeighbors tableAlgorithm = null;
		// PathNeighborTables queryTables = null;
		// PathNeighborTables graphTables = null;
		NeighborTables queryTables = null;
		NeighborTables graphTables = null;
		PruningAlgorithm pruningAlgorithm = null;
		NextQueryVertexes nqv = null;
		Isomorphism iso = null;
		float loadingTime = 0;
		float computingNeighborTime = 0;
		float pruningTime = 0;
		float isoTime = 0;
		int answerNum = -1;
		String temp[];
		temp = queryName.split("/");
		String outputDir = temp[temp.length - 1] + "_results";
		String comFile = "comparison.txt";
		BufferedWriter comBw = new BufferedWriter(new FileWriter(comFile, true));
		Long startingNode;
		Indexing ind = new Indexing();
		Selectivity sel = new Selectivity();
		ArrayList<InfoNode> infoNodes = new ArrayList<>();
		G = new BigMultigraph(graphName + "-sin.graph", graphName
				+ "-sout.graph");
		for (int exprimentTime = 0; exprimentTime < repititions; exprimentTime++) {
			StopWatch watch = new StopWatch();
			watch.start();
			
			System.out.println(temp[temp.length - 1]);
//
//			System.out.println("Data graph's vertexex number is "
//					+ G.vertexSet().size());
//			System.out.println("Data graph's maximum degree is "
//					+ ((BigMultigraph) G).getMaxDegree());
			// System.out.println("loading query");
			Q = new BigMultigraph(queryName, queryName, true);
			ind.indexing((BigMultigraph) G);
			// sel.print();
//			System.out.println("Query's vertexex number is "
//					+ Q.vertexSet().size());
//			System.out.println("Query's maximum degree is "
//					+ ((BigMultigraph) Q).getMaxDegree());

			// System.out.println(queryName);
			// System.out.println("query loaded");

			if (!this.isQueryMappable(Q)) {
				break;
			}
			loadingTime += watch.getElapsedTimeMillis();

			tableAlgorithm = new ComputeGraphNeighbors();
			// tableAlgorithm = new ComputePathGraphNeighbors();
//			Iterator<Long> iter = Q.vertexSet().iterator();
//			while (iter.hasNext()) {
//				System.out.println("==========================================");
				watch.reset();
				tableAlgorithm.setK(neighbourNum);
				tableAlgorithm.setGraph(G);
				tableAlgorithm.setNumThreads(threadsNum);
				tableAlgorithm.compute();
				graphTables = tableAlgorithm.getNeighborTables();
				tableAlgorithm.setGraph(Q);
				tableAlgorithm.compute();
				queryTables = tableAlgorithm.getNeighborTables();
				computingNeighborTime += watch.getElapsedTimeMillis();
				// System.out.println(queryTables.toString());
				watch.reset();
				 nqv = new NextQueryVertexes(G, Q, queryTables);
				 nqv.computeSelectivity();
				// startingNode = nqv.getNextVertexes();
				// startingNode = Q.vertexSet().iterator().next();
//				startingNode = ((BigMultigraph) Q).getStartingNode();
				startingNode = Q.vertexSet().iterator().next();
				InfoNode info = new InfoNode(startingNode);
//				System.out.println("starting node:" + startingNode);
				pruningAlgorithm = new PruningAlgorithm();
				// Set starting node according to sels of nodes.
				pruningAlgorithm.setStartingNode(startingNode);
				pruningAlgorithm.setGraph(G);
				pruningAlgorithm.setQuery(Q);
				pruningAlgorithm.setGraphTables(graphTables);
				pruningAlgorithm.setQueryTables(queryTables);
				// pruningAlgorithm.setGraphPathTables(graphTables);
				// pruningAlgorithm.setQueryPathTables(queryTables);
				pruningAlgorithm.setThreshold(threshold);
				pruningAlgorithm.compute();
				// pruningAlgorithm.fastCompute();
				
				info.setBsCount(pruningAlgorithm.getBsCount());
				info.setCmpCount(pruningAlgorithm.getCmpNbLabel());
				info.setUptCount(pruningAlgorithm.getUptCount());
				info.setSel(nqv.getNodeSelectivities().get(startingNode));
				infoNodes.add(info);
				queryGraphMapping = pruningAlgorithm.getQueryGraphMapping();
				// query nodes' candidate number
				// for(Entry<Long, Set<MappedNode>> en :
				// queryGraphMapping.entrySet()){
				// System.out.println(en.getKey() + "size: " +
				// en.getValue().size());
				// }
				// System.out.println("pruning time:" +
				// watch.getElapsedTimeMillis());
				pruningTime += watch.getElapsedTimeMillis();
//				System.out.println("Edge size:" + G.edgeSet().size());
				sel.setGraph((BigMultigraph) G);
				// sel.setGraph((BigMultigraph)Q);
				sel.setQuery((BigMultigraph) Q);
				sel.setIndexing(ind);
				sel.setQueryTables(queryTables);
				sel.setGraphTables(graphTables);
				sel.setPaths(pruningAlgorithm.getPaths());
				sel.setStartingNode(startingNode);
				sel.compute();
				// sel.computSelectivity(1, Q.vertexSet().iterator().next());
				// sel.computePruningCost(Q.vertexSet().iterator().next());
//				System.out.println("Pruning estimated cost:"
//						+ sel.getPruningCost());
				// sel.computSelectivity(1.0, Q.vertexSet().iterator().next());
//				System.out.println("starting node:" + startingNode);
//				for (int i = 0; i < pruningAlgorithm.getVisitSeq().size(); i++) {
//					System.out.println("node:"
//							+ pruningAlgorithm.getVisitSeq().get(i)
//							+ " candidates number:"
//							+ pruningAlgorithm.getCandidates().get(
//									pruningAlgorithm.getVisitSeq().get(i))
//							+ " estimated candidates:"
//							+ sel.getSels().get(
//									pruningAlgorithm.getVisitSeq().get(i)));
//				}
				// sel.print();
				// pruningAlgorithm.computeTimeCost();
//				System.out.println("Binary Search count:"
//						+ pruningAlgorithm.getBsCount());
//				System.out.println("Neighbourhood comparison count:"
//						+ pruningAlgorithm.getCmpNbLabel());
//				System.out.println("Update adjacent nodes count:"
//						+ pruningAlgorithm.getUptCount());
				watch.reset();
				
				HashSet<RelatedQuery> relatedQueriesUnique;
                List<RelatedQuery> relatedQueries;

//                IsomorphicQuerySearch isoAlgorithm = new IsomorphicQuerySearch();
                EditDistanceQuerySearch edAlgorithm = new EditDistanceQuerySearch();
                edAlgorithm.setStartingNode(startingNode);
                edAlgorithm.setQuery(Q);
                edAlgorithm.setGraph(G);
                edAlgorithm.setNumThreads(1);
                edAlgorithm.setQueryToGraphMap(pruningAlgorithm.getQueryGraphMapping());
                edAlgorithm.setLimitedComputation(false);
                edAlgorithm.setThreshold(threshold);
                edAlgorithm.compute();

                relatedQueries = edAlgorithm.getRelatedQueries();
                
//                IsomorphicQuerySearch edAlgorithm = new IsomorphicQuerySearch();
//                
//                edAlgorithm.setQuery(Q);
//                edAlgorithm.setGraph(G);
//                edAlgorithm.setNumThreads(12);
//                edAlgorithm.setQueryToGraphMap(pruningAlgorithm.getQueryGraphMapping());
//                edAlgorithm.setLimitedComputation(false);
//                edAlgorithm.compute();

//                relatedQueries = edAlgorithm.getRelatedQueries();

                relatedQueriesUnique = new HashSet<>(relatedQueries);
                int queriesCount = 0;
                for (RelatedQuery related : relatedQueriesUnique) {
                	System.out.println("count: " + queriesCount);
                	for (Edge re : related.getQuery().edgeSet()) {
                		System.out.println(re.getSource() + " " + re.getDestination() + " " + re.getLabel());
                	}
                	queriesCount++;
                }
//				iso = new Isomorphism();
//				iso.setStartingNode(startingNode);
//				iso.setQueryEdges(Q.edgeSet());
//				iso.setThreshold(threshold);
//				iso.setQuery(Q);
//				iso.setAnwserFile(queryName);
//				iso.setGraphName(graphName);
//				iso.setQueryGraphMapping(queryGraphMapping);
//				iso.setOutputDir(outputDir);
				// iso.findIsomorphism();
				// iso.getResultsWriter().close();
				comBw.write(temp[temp.length - 1]
						+ " "
						+ (pruningAlgorithm.getCmpNbLabel() + pruningAlgorithm
								.getBsCount())
						+ " "
						+ pruningAlgorithm.getUptCount()
						+ " "
						+ (pruningAlgorithm.getCmpNbLabel()
								+ pruningAlgorithm.getBsCount() + pruningAlgorithm
									.getUptCount()) + " "
						+ sel.getPruningCost() + " " + sel.getUpdateCost()
						+ " " + (sel.getUpdateCost() + sel.getPruningCost()));
				// comBw.write(temp[temp.length-1] + " "
				// +(pruningAlgorithm.getCmpNbLabel()+pruningAlgorithm.getBsCount()+pruningAlgorithm.getUptCount())+
				// " " + (sel.getUpdateCost()+sel.getPruningCost()));
				comBw.newLine();
				// FileOperator.mergeWildCardResults(outputDir,
				// Q.edgeSet().size());
//				answerNum = iso.getCount();
				isoTime += watch.getElapsedTimeMillis();
//			}
		}
//		System.out.println("==========================================");
		comBw.close();
//		Collections.sort(infoNodes, new NodeCostComparator());
//		Long tempID = infoNodes.get(0).getNodeID();
//		for(InfoNode in : infoNodes){
//			System.out.println(in.getNodeID() + " " + (in.getBsCount() + in.getCmpCount() + in.getUptCount()) + " " + in.getSel());
//		}
//		Collections.sort(infoNodes, new NodeExSelComparator());
//		if(!infoNodes.get(0).getNodeID().equals(tempID)){
//			System.out.println("High selectivity high cost:" + temp[temp.length - 1]);
//		}
		loadingTime = loadingTime / repititions;
		computingNeighborTime = computingNeighborTime / repititions;
		pruningTime = pruningTime / repititions;
		isoTime = isoTime / repititions;
//		System.out.println("Average loading time is " + loadingTime + " ms.");
//		System.out.println("Average neighbor computing time is "
//				+ computingNeighborTime + " ms.");
//		System.out.println("Average pruning time is " + pruningTime + " ms.");
//		System.out.println("Average isomorphism time is " + isoTime + " ms.");
//		BufferedWriter bw = new BufferedWriter((new OutputStreamWriter(
//				new FileOutputStream(outputFile, true))));
		// bw.write("threshold, loading time, neighbor computing time, pruning time, isomorphsim time"
		// );
		// bw.newLine();
		
		System.out.println("end");
//		bw.write(graphName + "," + queryName.substring(queryName.length() - 10)
//				+ ", " + threshold + ", " + loadingTime + ", "
//				+ computingNeighborTime + ", " + pruningTime + ", " + isoTime
//				+ ", " + answerNum);
//		bw.newLine();
//		bw.close();

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

}
