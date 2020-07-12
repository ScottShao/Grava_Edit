package eu.unitn.disi.db.grava.scc;

import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.command.util.StopWatch;
import eu.unitn.disi.db.exemplar.core.algorithms.ComputeGraphNeighbors;
import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.utils.AlgorithmName;
import eu.unitn.disi.db.grava.utils.FileOperator;
import eu.unitn.disi.db.grava.utils.Filter;
import eu.unitn.disi.db.grava.utils.MethodOption;
import eu.unitn.disi.db.tool.ThreadPoolFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;


public class Experiement {

    private int repititions;
    private int threshold;
    private int threadsNum;
    private int neighbourNum;
    private String graphName;
    private String queryFolder;
    private String outputFile;
    private String answerFile;
    private boolean isUsingWildCard;


    public Experiement() {
        // TODO Auto-generated constructor stub
    }

    public Experiement(int repititions, int threshold, int threadsNum, int neighbourNum, String graphName,
                       String queryFolder, String outputFile, boolean isUsingWildCard)
            throws AlgorithmExecutionException, ParseException, IOException {
        this.repititions = repititions;
        this.threshold = threshold;
        this.threadsNum = threadsNum;
        this.neighbourNum = neighbourNum;
        this.graphName = graphName;
        this.queryFolder = queryFolder;
        this.outputFile = outputFile;
        this.isUsingWildCard = isUsingWildCard;
    }

    public void runExperiement(AlgorithmName algorithmName, Filter filter) throws AlgorithmExecutionException, ParseException, IOException {
        System.out.println("Run experiment"  );
        StopWatch stopWatch = new StopWatch();

        EditDistance ed = new EditDistance();
        ed.setGraphName(graphName);
        ed.setNeighbourNum(neighbourNum);
        ed.setOutputFile(outputFile);
        ed.setRepititions(repititions);
        ed.setThreadsNum(threadsNum);

//		ed.setAnswerFile(answerFile);
        ArrayList<String> queryFiles = FileOperator.getFileName(queryFolder);
//		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(queryFolder+"/g" +graphName + "comparisonT" + threshold + ".csv"), true));
        int count = 0;
        int bsCount;
        int cmpCount;
        int uptCount;
        MethodOption mo = MethodOption.BOTH;
        ed.setMo(mo);
        ed.setThreshold(threshold);
//		ed.setCmpBw(bw);
//		ed.setCon(new Convertion());
//		ed.loadEntities();
        try {
//			bw.write("avg degree: 8.97, wc cost, ed cost, bf cost,exbf cost, wc time, ed time, bf time, exbf time");
//		bw.write("avg degree: 8.97, wc cost, ed cost, wc candidate, ed candidate, answer count, wc time, ex time, isWcBad, isEdBad, wcIntNum, wcIntSum, edIntNum");
//		bw.newLine();
//		List<String> strList = ed.readFile(queryFolder+"/comparison.csv");
//		ed.setStrList(strList);
            List<String> candList = new ArrayList<>();
            ed.setCandComp(candList);
//		List<String> selList = new ArrayList<>();
//		ed.setSelsComp(selList);
            StopWatch load = new StopWatch();
            load.start();
            Multigraph G = new BigMultigraph(graphName + "-sin.graph", graphName
                    + "-sout.graph" );
            System.out.println("compute neighbourhood" );
            ComputeGraphNeighbors tableAlgorithm = new ComputeGraphNeighbors();
            tableAlgorithm.setK(neighbourNum);
            tableAlgorithm.setGraph(G);
            tableAlgorithm.setNumThreads(threadsNum);
            tableAlgorithm.setNodePool(ThreadPoolFactory.getTableComputeThreadPool());
            if (Filter.NEIGHBOUR == filter || Filter.BOTH == filter) {
                tableAlgorithm.compute();
            }
            if (Filter.PATH == filter || Filter.BOTH == filter) {
                tableAlgorithm.computePathFilter();
            }
            System.out.println("loading graph takes " + load.getElapsedTimeMillis());

            stopWatch.start();
            ed.setgTableAlgorithm(tableAlgorithm);
            ed.setG(G);
            for (String queryFile : queryFiles) {
                ed.setQueryName(queryFile);
                ed.runEditDistance(algorithmName, filter);
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
//			bw.close();
        }
        System.out.println("threashold: " + threshold);
        System.out.println("total time: " + (double)stopWatch.getElapsedTimeSecs() / queryFiles.size());

//		for(int i = 0; i < 50; i++){
//			bsCount = 0;
//			cmpCount = 0;
//			uptCount = 0;
//			ed.setThreshold(threshold);
////			ed.setQueryName(queryFolder + "/" + "query" + i + ".txt");
////			ed.runEditDistance();
//			
////			ed.setQueryName(queryFolder + "/" + "Clique" + i + ".txt");
////			ed.runEditDistance();
//			ed.setQueryName(queryFolder + "/" + "E2FQ" + i + ".txt");
//			ed.runEditDistance();
//			bsCount += ed.getBsCount();
//			cmpCount += ed.getCmpCount();
//			uptCount += ed.getUptCount();
//          System.out.println(bsCount);
//          System.out.println(cmpCount);
//          System.out.println(uptCount);
//		}
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


    public String getQueryFolder() {
        return queryFolder;
    }

    public void setQueryFolder(String queryFolder) {
        this.queryFolder = queryFolder;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }


}
