package eu.unitn.disi.db.grava.scc;

import java.io.IOException;

import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.query.WildCardQuery;

public class Main {

	public static void main(String[] args) throws AlgorithmExecutionException, ParseException, IOException {
		if(args.length == 8){
			int repititions = Integer.parseInt(args[0]);
			int threshold = Integer.parseInt(args[1]);
			int threadsNum = Integer.parseInt(args[2]);
			int neighbourNum = Integer.parseInt(args[3]);
			String graphName = args[4];
			String queryFolder = args[5];
			String outputFile = args[6];
			boolean isUsingWildCard = Boolean.parseBoolean(args[7]);
			Experiement exp = new Experiement(repititions, threshold, threadsNum, neighbourNum, graphName, queryFolder, outputFile, isUsingWildCard);
			exp.runExperiement();
		}else{
			System.err.println("Not enough parameters, please enter parameter again");
		}

	}

}
