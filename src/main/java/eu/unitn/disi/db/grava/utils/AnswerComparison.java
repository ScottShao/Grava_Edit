package eu.unitn.disi.db.grava.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import eu.unitn.disi.db.grava.graphs.Answer;

public class AnswerComparison {
	private int edgeNum;
	private BufferedReader br;
	private HashSet<Answer> answers;
	
	public AnswerComparison() {
		this.edgeNum = -1;
	}
	
	public AnswerComparison(int edgeNum) {
		this.edgeNum = edgeNum;
		this.answers = new HashSet<>();
		
	}
	
	public void loadFirstFile(String firstFile)throws IOException {
		this.br = new BufferedReader(new FileReader(firstFile));
		String context = null;
		Answer a;
		int line = 1;
		int remainder = edgeNum+1;
		ArrayList<String> edges =  new ArrayList<>();;
		context = br.readLine();
		line++;
		while((context = br.readLine()) != null){
			if(line%remainder == 1){
				a = new Answer(edges);
				answers.add(a);
				edges = new ArrayList<>();
				line++;
				continue;
			}else{
				line++;
				edges.add(context);
			}
		}
		a = new Answer(edges);
		answers.add(a);
		br.close();
	}
	
	public void appendAnotherResults(String secFile) throws IOException{
		this.br = new BufferedReader(new FileReader(secFile));
		String context = null;
		int line = 1;
		int remainder = edgeNum+1;
		Answer a;
		ArrayList<String> edges =  new ArrayList<>();;
		context = br.readLine();
		line++;
		int count = 0;
		while((context = br.readLine()) != null){
			if(line%remainder == 1){
				a = new Answer(edges);
				answers.add(a);
				edges = new ArrayList<>();
				line++;
				continue;
			}else{
				line++;
				edges.add(context);
			}
		}
		a = new Answer(edges);
		answers.add(a);
	}

	public HashSet<Answer> getAnswers() {
		return answers;
	}

	public void setAnswers(HashSet<Answer> answers) {
		this.answers = answers;
	}
	
	

}
