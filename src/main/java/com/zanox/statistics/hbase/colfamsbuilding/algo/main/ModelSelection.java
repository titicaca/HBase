package com.zanox.statistics.hbase.colfamsbuilding.algo.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.zanox.statistics.hbase.colfamsbuilding.algo.core.Constants;
import com.zanox.statistics.hbase.colfamsbuilding.algo.core.Environment;
import com.zanox.statistics.hbase.colfamsbuilding.algo.core.Individual;
import com.zanox.statistics.hbase.colfamsbuilding.algo.core.Log;

public class ModelSelection{	
//	static float []copulationRates = {(float) 0.1, (float) 0.2, (float) 0.3 };
//	static float []mutationRates = { (float) 0.3, (float) 0.5, (float) 0.7};
//	static float []crossOverRates = { (float) 0.05, (float) 0.1, (float) 0.15};
//	static float []crossOver2Rates = { 0, (float) 0.05, (float) 0.1};
//	static float []splitUpRates = {(float) 0.1, (float) 0.2, (float) 0.3};
//	static float []mergeRates = {(float) 0.1, (float) 0.2, (float) 0.3};
	
	static float []copulationRates = {(float) 0.05, (float) 0.1, (float) 0.15 };
	static float []mutationRates = { (float) 0.3, (float) 0.5, (float) 0.7};
	static float []crossOverRates = { (float) 0.1, (float) 0.2, (float) 0.3};
	static float []crossOver2Rates = { (float) 0.05, (float) 0.1, (float) 0.15};
	static float []splitUpRates = {(float) 0.1, (float) 0.2, (float) 0.3};
	static float []mergeRates = {(float) 0.1, (float) 0.2, (float) 0.3 };

	
//	static float []copulationRates = {(float) 0.1 };
//	static float []mutationRates = { (float) 0.5};
//	static float []crossOverRates = {(float) 0.2};
//	static float []crossOver2Rates = { 0, (float) 0.01, (float)0.02, (float)0.03};
//	static float []splitUpRates = {(float) 0.02};
//	static float []mergeRates = {(float) 0.05};
		
	static int iteration = 500;
	
	//Environment: CopulationRate-0.1 MutationRate-0.5 CrossOverRate-0.2 CrossOver2Rate-0.0 SplitUpRate-0.02 MergeRate-0.05
	
	public static void main(String []args) throws IOException{
		List<Environment> environments = new ArrayList<Environment>();
		List<Float> trainCosts = new ArrayList<Float>();
		List<Float> validationCosts = new ArrayList<Float>();
		List<Float> testCosts = new ArrayList<Float>();
		
		Log log = new Log("modelSelection_500");
		log.start(false);
		
		int id = 0;
		
		for(float copulationRate : copulationRates){
			for(float mutationRate : mutationRates){
				for(float crossOverRate : crossOverRates){
					for(float crossOver2Rate : crossOver2Rates){
						for(float splitUpRate : splitUpRates){
							for(float mergeRate : mergeRates){
								id ++;
								Environment e = new Environment(copulationRate, 
										mutationRate,
										crossOverRate,
										crossOver2Rate,
										splitUpRate,
										mergeRate);
								environments.add(e);
								Individual i = e.train(iteration);
//								System.out.println(i.toString());
								float trainCost = i.getCost();
								float validationCost = e.validation(i);
								float testCost = e.test(i);
								
								trainCosts.add(trainCost);
								validationCosts.add(validationCost);
								testCosts.add(testCost);
								
								String logString = ("\n----------model " + id + "----------\n")
													+(e.toString())
													+("train: " + trainCost) + "\n"
													+("validate: " + validationCost) + "\n"
													+("test: " + testCost) + "\n\n"
													+ i.toString()
													+ ("-------------------------") +"\n";
								log.printLog(logString);
								log.flush();
								System.out.print(logString);
							}
						}
					}
				}
			}
		}
		
		//find a e with minimun validation cost
		float minValidationCost =0;
		int minIndex = 0;
		for(int i = 0; i < validationCosts.size(); i ++){
			if(i == 0){
				minValidationCost = validationCosts.get(i);
			}else{
				if(minValidationCost > validationCosts.get(i)){
					minValidationCost = validationCosts.get(i);
					minIndex = i;
				}
			}
		}
		

		String logString = ("\n----------best model " + (minIndex + 1) + "-----------\n")
			+(environments.get(minIndex).toString())
			+("train: " + trainCosts.get(minIndex)) + "\n"
			+("validate: " + validationCosts.get(minIndex)) + "\n"
			+("test: " + testCosts.get(minIndex)) + "\n\n"
			+ ("--------------------") +"\n";
		
		log.printLog(logString);
		log.flush();

		System.out.print(logString);
		log.end();
		
	
	}
}