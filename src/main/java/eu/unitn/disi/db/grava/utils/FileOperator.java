package eu.unitn.disi.db.grava.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

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
			if(fileList[i].getName().equals(".DS_Store")){
				continue;
			}
			queryFiles.add(fileList[i].getAbsolutePath());
		}
		return queryFiles;
	}
	
}
