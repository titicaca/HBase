package com.zanox.statistics.hbase.colfamsbuilding.algo.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.zanox.statistics.hbase.colfamsbuilding.algo.core.Constants;
import com.zanox.statistics.hbase.colfamsbuilding.algo.core.Environment;
import com.zanox.statistics.hbase.colfamsbuilding.algo.core.Individual;
import com.zanox.statistics.hbase.colfamsbuilding.algo.core.Log;

/**
 * ----------model 242----------
 * Environment: CopulationRate-0.1 MutationRate-0.7 CrossOverRate-0.15 CrossOver2Rate-0.1 SplitUpRate-0.3 MergeRate-0.2
 * train: 0.22461192
 * validate: 0.22463973
 * test: 0.22455174
 * 
 * @author fangzhou.yang
 *
 */
public class Training{
	static int iteration = 500;
	static float copulationRate = (float) 0.1;
	static float mutationRate = (float)0.7;
	static float crossOverRate = (float)0.15;
	static float crossOver2Rate = (float) 0.1;
	static float splitUpRate = (float) 0.3;
	static float mergeRate = (float) 0.2;

	public static void main(String []args) throws IOException{
		Constants.isELog = true;

		Log log = new Log("training log");
		log.start(false);
		
		
		Environment e = new Environment(copulationRate, 
				mutationRate,
				crossOverRate,
				crossOver2Rate,
				splitUpRate,
				mergeRate);
		Individual i = e.train(iteration);
//		System.out.println(i.toString());
		float trainCost = i.getCost();
		float validationCost = e.validation(i);
		float testCost = e.test(i);
		
								
		String logString = ("\n----------Training Model----------\n")
		+(e.toString())
		+("train: " + trainCost) + "\n"
		+("validate: " + validationCost) + "\n"
		+("test: " + testCost) + "\n\n"
		+ i.toString()
		+ ("-------------------------") +"\n";
		log.printLog(logString);
		log.flush();
		System.out.print(logString);						

		log.end();
	}
}