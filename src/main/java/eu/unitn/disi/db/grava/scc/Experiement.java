package eu.unitn.disi.db.grava.scc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import eu.unitn.disi.db.grava.utils.MethodOption;
import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.exemplar.core.RelatedQuery;
import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.utils.FileOperator;
import eu.unitn.disi.db.grava.utils.Utilities;
import eu.unitn.disi.db.query.WildCardQuery;


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
	
	public Experiement(int repititions, int threshold, int threadsNum, int neighbourNum, String graphName, String queryFolder, String outputFile, boolean isUsingWildCard) throws AlgorithmExecutionException, ParseException, IOException{
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
	
	public void runExperiement() throws AlgorithmExecutionException, ParseException, IOException{
		
		EditDistance ed = new EditDistance();
		ed.setGraphName(graphName);
		ed.setNeighbourNum(neighbourNum);
		ed.setOutputFile(outputFile);
		ed.setRepititions(repititions);
		ed.setThreadsNum(threadsNum);
		
//		ed.setAnswerFile(answerFile);
		ArrayList<String> queryFiles = FileOperator.getFileName(queryFolder);
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(queryFolder+"/comparison.csv"), true));
		int count = 0;
		int bsCount;
		int cmpCount;
		int uptCount;
		MethodOption mo = MethodOption.BOTH;
		ed.setMo(mo);
		ed.setThreshold(threshold);
		ed.setCmpBw(bw);
		try{
		bw.write("avg degree: 8.97, wc candidates, wc estimated candidates, ex candidates, ex estimated candidates, wc cost, wc estimated cost, ex cost, ex estimated cost, wc running time, ex running time");
		bw.newLine();
		
		for (String queryFile : queryFiles) {
			if (queryFile.contains("csv")) {
				continue;
			}
			ed.setQueryName(queryFile);
			ed.runEditDistance();
		}
		
		}catch (IOException ioe) {
			ioe.printStackTrace();
		}finally {
			bw.close();
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
