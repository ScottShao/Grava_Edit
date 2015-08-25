package eu.unitn.disi.db.grava.utils;

import java.io.File;
import java.util.ArrayList;

public class FileOperator {
	
	public FileOperator() {
		
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
