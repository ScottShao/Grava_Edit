package eu.unitn.disi.db.grava.scc;

import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.exemplar.core.algorithms.ComputeGraphNeighbors;
import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.utils.FileOperator;
import eu.unitn.disi.db.grava.utils.MethodOption;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


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
//		this.answerFile = answerFile;
    }

    public void runExperiement() throws AlgorithmExecutionException, ParseException, IOException {
        System.out.println("Run experiment" );
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
            double start = System.nanoTime();
            Multigraph G = new BigMultigraph(graphName + "-sin.graph", graphName
                    + "-sout.graph" );
            System.out.println("compute neighbourhood" );
            ComputeGraphNeighbors tableAlgorithm = new ComputeGraphNeighbors();
            tableAlgorithm.setK(neighbourNum);
            tableAlgorithm.setGraph(G);
            tableAlgorithm.setNumThreads(threadsNum);
            tableAlgorithm.compute();
            System.out.println(System.nanoTime() - start);
//		tableAlgorithm.computePathFilter();
            System.out.println("loading graph takes " + (System.nanoTime() - start));
//		HashMap<Long, LabelContainer> labelFreq = G.getLabelFreq();
//		Map<Connection, int[]> conCount = tableAlgorithm.getConCount();
//		TreeSet<Connection> ts = new TreeSet<>(new Comparator<Connection>(){
//
//			@Override
//			public int compare(Connection o1, Connection o2) {
//				double t1 = o1.getFreq() / (double)o1.getFirstFreq();
//				double t2 = o2.getFreq() / (double)o2.getFirstFreq();
//				if (t2 > t1) {
//					return -1;
//				} else {
//					return 1;
//				}
//			}
//			
//		});
//		for (Entry<Connection, int[]> cc : conCount.entrySet()) {
//			Connection temp = cc.getKey();
//			temp.setFreq(cc.getValue()[0]);
//			long first = temp.getFirst();
//			first = first > 0 ? first : -first;
//			temp.setFirstFreq(labelFreq.get(first).getFrequency());
//			ts.add(temp);
//		}
//		char qc = 'a';
//		int lvl = 1;
//		String fn = "a";
//		BufferedWriter freqBW = new BufferedWriter(new FileWriter(new File("./test/test/freq.txt"), true));
//		Random rn = new Random();
            /**
             while (!ts.isEmpty()) {
             Connection c = ts.pollFirst();
             if (rn.nextDouble() < 0.015) {
             BufferedWriter queryBW = new BufferedWriter(new FileWriter(new File("./test/test/" + fn + ".txt")));
             queryBW.write("1 2 " + 1000008979);
             queryBW.newLine();
             queryBW.write("3 2 " + 1000009041);
             queryBW.newLine();
             if (c.getFirst() > 0) {
             queryBW.write("1 4 " + c.getFirst());
             } else {
             queryBW.write("4 1 " + (-c.getFirst()));
             }
             queryBW.newLine();
             if (c.getSecond() > 0) {
             queryBW.write("4 5 " + c.getSecond());
             } else {
             queryBW.write("5 4 " + (-c.getSecond()));
             }
             queryBW.newLine();
             freqBW.write(fn + "," + 418 + "," + c.getFreq() /(double)c.getFirstFreq());
             freqBW.newLine();
             freqBW.flush();
             queryBW.flush();
             queryBW.close();
             lvl++;
             if (lvl > 20) {
             lvl = 1;
             qc++;
             }
             fn = "";
             for (int i = 0; i < lvl; i++) {
             fn += qc;
             }
             }
             //			System.out.println(c.getFirst() + "==" + c.getSecond() + ":" + c.getFreq() /(double)c.getFirstFreq()) ;
             }
             freqBW.close();
             **/
            ed.setgTableAlgorithm(tableAlgorithm);
            ed.setG(G);
            for (String queryFile : queryFiles) {
                if (queryFile.contains("csv" ) || !queryFile.contains("E8E67504984819548.txt")) {
                    continue;
                }
                ed.setQueryName(queryFile);
                ed.runEditDistance();
            }
//		ed.write(queryFolder+"/c.csv");
//		ed.writeCand(queryFolder +"/cand.csv");
//		ed.writeSels(queryFolder + "/sels.csv");
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
//			bw.close();
        }

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
