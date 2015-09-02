package eu.unitn.disi.db.grava.scc;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.exemplar.core.algorithms.ComputeGraphNeighbors;
import eu.unitn.disi.db.exemplar.core.algorithms.ComputePathGraphNeighbors;
import eu.unitn.disi.db.exemplar.core.algorithms.PruningAlgorithm;
import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.utils.FileOperator;
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
	public EditDistance() throws AlgorithmExecutionException, ParseException, IOException{
	}
	
	public EditDistance(int repititions, int threshold, int threadsNum, int neighbourNum, String graphName, String queryName, String outputFile, String answerFile) throws AlgorithmExecutionException, ParseException, IOException{
		this.repititions = repititions;
		this.threshold = threshold;
		this.threadsNum = threadsNum;
		this.neighbourNum = neighbourNum;
		this.graphName = graphName;
		this.queryName = queryName;
		this.outputFile = graphName + "_output/" +outputFile;
		this.answerFile = answerFile;
		
	}
	
	public void runEditDistance() throws ParseException, IOException,
			AlgorithmExecutionException {
		
		Map<Long, Set<MappedNode>> queryGraphMapping = null;
//		ComputeGraphNeighbors tableAlgorithm = null;
		ComputePathGraphNeighbors tableAlgorithm = null;
		PathNeighborTables queryTables = null;
		PathNeighborTables graphTables = null;
//		NeighborTables queryTables = null;
//		NeighborTables graphTables = null;
		PruningAlgorithm pruningAlgorithm = null;
		Isomorphism iso = null;
		float loadingTime = 0;
		float computingNeighborTime = 0;
		float pruningTime = 0;
		float isoTime = 0;
		int answerNum = -1;
		String outputDir = queryName.substring(0,queryName.length()-11) + "_results";
		
		for (int exprimentTime = 0; exprimentTime < repititions; exprimentTime++) {
			StopWatch watch = new StopWatch();
			watch.start();
			
			G = new BigMultigraph(graphName+"-sin.graph", graphName+"-sout.graph");
//			System.out.println("loading query");
			Q = new BigMultigraph(queryName, queryName, true);
//			System.out.println(queryName);
//			System.out.println("query loaded");
			System.out.println(queryName.substring(queryName.length()-10));
			if(!this.isQueryMappable(Q)){
				break;
			}
			loadingTime += watch.getElapsedTimeMillis();

//			tableAlgorithm = new ComputeGraphNeighbors();
			tableAlgorithm = new ComputePathGraphNeighbors();

			watch.reset();
			tableAlgorithm.setK(neighbourNum);
			tableAlgorithm.setGraph(G);
			tableAlgorithm.setNumThreads(threadsNum);
			tableAlgorithm.compute();
			graphTables = tableAlgorithm.getPathNeighborTables();
			tableAlgorithm.setGraph(Q);
			tableAlgorithm.compute();
			queryTables = tableAlgorithm.getPathNeighborTables();
			computingNeighborTime += watch.getElapsedTimeMillis();
			
			watch.reset();
			pruningAlgorithm = new PruningAlgorithm();
			pruningAlgorithm.setGraph(G);
			pruningAlgorithm.setQuery(Q);
//			pruningAlgorithm.setGraphTables(graphTables);
//			pruningAlgorithm.setQueryTables(queryTables);
			pruningAlgorithm.setGraphPathTables(graphTables);
			pruningAlgorithm.setQueryPathTables(queryTables);
			pruningAlgorithm.setThreshold(threshold);
			pruningAlgorithm.compute();

			queryGraphMapping = pruningAlgorithm.getQueryGraphMapping();
//			System.out.println("pruning time:" + watch.getElapsedTimeMillis());
			pruningTime += watch.getElapsedTimeMillis();
			
			watch.reset();
			iso = new Isomorphism();
			iso.setQueryEdges(Q.edgeSet());
			iso.setThreshold(threshold);
			iso.setQuery(Q);
			iso.setAnwserFile(queryName);
			iso.setGraphName(graphName);
			iso.setQueryGraphMapping(queryGraphMapping);
			iso.setOutputDir(outputDir);
			iso.findIsomorphism();
			iso.getResultsWriter().close();
			
			FileOperator.mergeWildCardResults(outputDir, Q.edgeSet().size());
			answerNum = iso.getCount();
			isoTime += watch.getElapsedTimeMillis();
		}
		
		loadingTime = loadingTime/repititions;
		computingNeighborTime = computingNeighborTime/repititions;
		pruningTime = pruningTime/repititions;
		isoTime = isoTime/repititions;
		System.out.println("Average loading time is " + loadingTime + " ms.");
		System.out.println("Average neighbor computing time is " + computingNeighborTime + " ms.");
		System.out.println("Average pruning time is " + pruningTime + " ms.");
		System.out.println("Average isomorphism time is " + isoTime + " ms.");
		BufferedWriter bw = new BufferedWriter((new OutputStreamWriter(   
                new FileOutputStream(outputFile, true))));
//		bw.write("threshold, loading time, neighbor computing time, pruning time, isomorphsim time" );
//		bw.newLine();
		
		bw.write(graphName + "," + queryName.substring(queryName.length()-10) + ", " + threshold + ", " + loadingTime + ", " + computingNeighborTime + ", " + pruningTime + ", " + isoTime + ", " + answerNum);
		bw.newLine();
		bw.close();
		
	}
	
	protected boolean isQueryMappable(Multigraph queryGraph) {

        for (Long node : queryGraph.vertexSet()) {
            if (queryGraph.outDegreeOf(node) > G.outDegreeOf(node)) {
            	System.out.println(queryName + " is not mappable");
            	return false;
            }

            Collection<Edge> edges = G.outgoingEdgesOf(node);

            for (Edge e : queryGraph.outgoingEdgesOf(node)) {
            	if(e.getLabel().equals(0L)){
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
