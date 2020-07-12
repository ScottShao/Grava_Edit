package eu.unitn.disi.db.grava.scc;

import static eu.unitn.disi.db.grava.utils.AlgorithmName.EXED;
import static eu.unitn.disi.db.grava.utils.AlgorithmName.WCED;

import eu.unitn.disi.db.grava.utils.Filter;
import eu.unitn.disi.db.tool.ThreadPoolFactory;
import java.io.IOException;

import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.query.WildCardQuery;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
//			for (int i = 1; i <= 3; i++) {
//				Experiement exp = new Experiement(repititions, i, threadsNum, neighbourNum, graphName,
//						queryFolder, outputFile, isUsingWildCard);
//				exp.runExperiement(Filter.NEIGHBOUR);
//			}
			for (int i = 1; i <= 1; i++) {
				Experiement exp = new Experiement(repititions, i, threadsNum, neighbourNum, graphName,
						queryFolder, outputFile, isUsingWildCard);
				exp.runExperiement(EXED, Filter.PATH);
			}

//            for (int i = 1; i <= 3; i++) {
//                Experiement exp = new Experiement(repititions, i, threadsNum, neighbourNum, graphName,
//                        queryFolder, outputFile, isUsingWildCard);
//                exp.runExperiement(Filter.PATH);
//            }

//			for (int i = 3; i <= 3; i++) {
//				Experiement exp = new Experiement(repititions, i, threadsNum, neighbourNum, graphName,
//						queryFolder, outputFile, isUsingWildCard);
//				exp.runExperiement(Filter.NEIGHBOUR);
//			}



//            for (int i = 3; i <= 3; i++) {
//                Experiement exp = new Experiement(repititions, i, threadsNum, neighbourNum, graphName,
//                        queryFolder, outputFile, isUsingWildCard);
//                exp.runExperiement(Filter.NEIGHBOUR);
//            }

//
//			for (int i = 1; i <= 3; i++) {
//				Experiement exp = new Experiement(repititions, i, threadsNum, neighbourNum, graphName,
//						queryFolder, outputFile, isUsingWildCard);
//				exp.runExperiement(Filter.BOTH);
//			}
			ThreadPoolFactory.shutdownAll();
		}else{
			System.err.println("Not enough parameters, please enter parameter again");
		}
	}

}
