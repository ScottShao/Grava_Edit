package eu.unitn.disi.db.grava.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import eu.unitn.disi.db.grava.graphs.Answer;

public class FileOperator {
	
	public FileOperator() {
		
	}
	public static void createDir(String dirName){
		File dir = new File(dirName);
		if(!dir.exists()){
			dir.mkdirs();
		}
	}
	public static ArrayList readQuery(File queryFile) throws IOException{
		ArrayList<String> query = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader(queryFile));
		String line = null;
		while((line = br.readLine()) != null){
			query.add(line);
		}
		return query;
	}
	
	public static ArrayList getFileName(String folderName){
		File folder = new File(folderName);
		File[] fileList = folder.listFiles();
		ArrayList<String> queryFiles = new ArrayList<String>();
		for(int i = 0; i < fileList.length; i++){
			if(fileList[i].isDirectory()){
				continue;
			}
			if(fileList[i].getName().equals(".DS_Store")){
				continue;
			}
			queryFiles.add(fileList[i].getAbsolutePath());
		}
		return queryFiles;
	}
	
	public static void mergeWildCardResults(String wildCardDir, int edgeNum) throws IOException{
		ArrayList<String> fileNames = getFileName(wildCardDir);
		AnswerComparison ac = new AnswerComparison(edgeNum);
		if(fileNames.size() == 0){
			System.err.println("no wild card files to merge");
		}else{
			ac.loadFirstFile(fileNames.get(0));
			for(int i = 1; i < fileNames.size(); i ++){
				ac.appendAnotherResults(fileNames.get(i));
			}
			writeTotalResults(ac.getAnswers(), wildCardDir +"/" + "total_results.txt");
		}
	}
	
	public static void writeTotalResults(HashSet<Answer> answers, String fileName) throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
		int count = 0;
		for(Answer ans : answers){
			bw.write("query solution " + count);
			bw.newLine();
			bw.write(ans.toString());
			count ++;
		}
		bw.close();
	}
	
}
