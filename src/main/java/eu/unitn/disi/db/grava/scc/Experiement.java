package eu.unitn.disi.db.grava.scc;

import java.io.IOException;
import java.util.ArrayList;

import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.utils.FileOperator;
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
		for(int i = 0; i < 1; i++){
			ed.setThreshold(threshold);
			ed.setQueryName(queryFolder + "/" + "E5PQ" + i + ".txt");
			ed.runEditDistance();
//			ed.setQueryName(queryFolder + "/" + "E5FQ" + i + ".txt");
//			ed.runEditDistance();
		}
//		for(String queryFile : queryFiles){
//			if(threshold != 0 && isUsingWildCard){
//				WildCardQuery wcq = new WildCardQuery(1);
//				wcq.run(queryFile);
//				ArrayList<String> wildCardFiles = FileOperator.getFileName(wcq.getDirName());
//				ed.setThreshold(0);
//				for(String wildCardQuery : wildCardFiles){
//					ed.setQueryName(wildCardQuery);
//					ed.runEditDistance();
//				}
//			}else{
//				if(queryFile.contains("E2")){
//					System.out.println(queryFile);
//					ed.setThreshold(threshold);
//					ed.setQueryName(queryFile);
//					ed.runEditDistance();
//				}
//			}
//			
////			System.out.println("queryfile:" +queryFile);
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
