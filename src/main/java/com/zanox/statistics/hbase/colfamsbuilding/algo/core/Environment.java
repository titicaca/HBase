package com.zanox.statistics.hbase.colfamsbuilding.algo.core;

import java.io.IOException;


public class Environment{
	float copulationRate = Constants.RATE_CopulateRate;
//	float evolutionRate = Constants.RATE_evolutionRate;
	float mutationRate = Constants.RATE_mutationRate;
	float crossOverRate = Constants.RATE_crossOverRate;
	float crossOver2Rate = Constants.RATE_crossOver2Rate;
	float splitUpRate = Constants.RATE_splitUpRate;
	float mergeRate = Constants.RATE_mergeRate;
		
	public Environment(float copulation_Rate, 
//			float evolution_Rate, 
			float mutation_Rate, 
			float crossOver_Rate, 
			float crossOver2_Rate, 
			float splitUp_Rate, 
			float merge_Rate ){
		copulationRate = copulation_Rate;
//		evolutionRate= evolution_Rate;
		mutationRate = mutation_Rate;
		crossOverRate = crossOver_Rate;
		crossOver2Rate = crossOver2_Rate;
		splitUpRate = splitUp_Rate;
		mergeRate = merge_Rate;
	}
	
	public Environment() {
		// using default rates in Constants
	}
	
	public Individual train(int iteration) throws IOException{
		DataQueries.initQueryPrototypesFromFile(Constants.Training_QueryPrototypes_FILE_NAME, ",");
		//DataQueries.printQueries();
		
		long startTime=System.nanoTime();

		float[] avgCost = new float [iteration];
		float[] avgColFamConsume = new float [iteration];
		float[] avgSkew = new float [iteration];
		float[] avgSumAtts = new float [iteration];
		float[] avgLoadBalance = new float [iteration];
//		float[] temperature = new float [iteration];
//		float[] alpha = new float [iteration];
		float[] rank = new float [iteration];
		float[] avgDuplicate = new float [iteration];
	
		float min_m = (float) 1.0;
		float max_m = (float) 4.0;
		Population pop= new Population(Constants.Init_Duplicate, 100);
		pop.setRate(this);
		
		if(Constants.isELog){
			Constants.eLog.start(false);
		}
		
		pop.printLog_parameters();
		//pop.display();
		System.out.print(this.toString());
		System.out.println("Initiation done..\n");
		System.out.println("Learning Processing.. Please wait..");
		try{
			for(int i = 0; i < iteration; i++){
								
//				alpha[i] = Constants.FACTOR_Alpha;
//				temperature[i] = 1 / Constants.FACTOR_Alpha * 10;
				//System.out.println("Evolution " + i);
				Constants.eLog.printLogln("Evolution " + i);
				pop.evolution();
				//System.out.println("Selection " + i);
				Constants.eLog.printLogln("Selection " + i);
				
				//pop.natureSelection();
				float m = min_m + (max_m - min_m) * i / iteration;
				//pop.natureSelection_eRANKING(m);
				rank[i] = m;
				pop.natureSelection_Roulette_eRank(m);
				
				avgCost[i] = pop.avgCost;
				avgColFamConsume[i] = pop.avgColFamConsume;
				avgSkew[i] = pop.avgSkew;
				avgSumAtts[i] = pop.avgSumAtts;
				avgLoadBalance[i] = pop.avgLoadBalance;
				avgDuplicate[i] = pop.avgDuplicateRate;
				
				if(i%100==0){
					System.out.print(i + " ");
				}
				Constants.eLog.flush();
				
			}
			//pop.display();
			
			Log l = new Log(Constants.Curve_FILE_NAME);
			l.start(false);
			l.printLogln("Cost; ColFamConsume; Skew; SumAtts; LoadBalance; Duplicate; rank m");
			for(int i = 0; i < iteration; i++){
				l.printLogln(avgCost[i] + ";" + avgColFamConsume[i] + ";" +avgSkew[i] + ";" + avgSumAtts[i] + ";" + avgLoadBalance[i] +";" + avgDuplicate[i] + ";" + rank[i]);
			}
			l.end();	
			if(Constants.isELog){
				Constants.eLog.end();
			}
			
			long endTime=System.nanoTime();
			
			System.out.print("\n");
			System.out.println("Learning Time: " + ((endTime - startTime)/1000/1000/1000) + " s");
			
			
		}catch(Exception e){
			e.printStackTrace();
			if(Constants.isELog){
				Constants.eLog.end();
			}
		}	
		return pop.getBestIndividual();
	}
	
	public float validation(Individual best) throws IOException{
		DataQueries.initQueryPrototypesFromFile(Constants.Validation_QueryPrototypes_FILE_NAME, ",");
		if(best == null){
			return -1;
		}
		best.calculateCost();
		return best.cost;		
	}
	
	public float test(Individual best) throws IOException{
		DataQueries.initQueryPrototypesFromFile(Constants.Testing_QueryPrototypes_FILE_NAME, ",");
		if(best == null){
			return -1;
		}
		best.calculateCost();
		return best.cost;	
	}
	
	@Override
	public String toString(){
		String s = "Environment: CopulationRate-" + this.copulationRate 
			+ " MutationRate-" + this.mutationRate
			+ " CrossOverRate-" + this.crossOverRate
			+ " CrossOver2Rate-" + this.crossOver2Rate
			+ " SplitUpRate-" + this.splitUpRate
			+ " MergeRate-" + this.mergeRate
			+ "\n";
		return s;	
	}
	
	
}