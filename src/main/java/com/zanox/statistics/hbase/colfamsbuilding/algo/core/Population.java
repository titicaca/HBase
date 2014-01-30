package com.zanox.statistics.hbase.colfamsbuilding.algo.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import com.zanox.statistics.hbase.colfamsbuilding.algo.types.Attribute;
import com.zanox.statistics.hbase.colfamsbuilding.algo.types.ColFam;

public class Population{
	int size = 0;
	int time = 0;
	float avgCost = 0;
	float avgSkew = 0;
	float avgColFamConsume = 0;
	int avgSumAtts = 0;
	float avgLoadBalance = 0;
	float avgDuplicateRate = 0;
	
	//defaut rate
	float copulationRate = Constants.RATE_CopulateRate;
	float evolutionRate = Constants.RATE_evolutionRate;
	float mutationRate = Constants.RATE_mutationRate;
	float crossOverRate = Constants.RATE_crossOverRate;
	float crossOver2Rate = Constants.RATE_crossOver2Rate;
	float splitUpRate = Constants.RATE_splitUpRate;
	float mergeRate = Constants.RATE_mergeRate;
	
	int mutationMaxAttNum = Constants.NUM_MutationMaxAttNum;
	
	int attSize = Constants.NUM_AttributeSize; //num of Atts
	
	Random rand = new Random(100);
	
	ArrayList<Individual> group = new ArrayList<Individual> ();
	
	int currentID = 0;	
	
	
	Population(int duplicate, int seed){
		System.out.println("initialization..");
			initialization(duplicate);
			 rand = new Random(seed);
		System.out.println("initialization done!");

	}
	
	
	
	public void initialization(int duplicate){
		//two colfams
		for(int i = 0; i < Constants.NUM_OriginPopulationSize/2; i++){
			currentID ++;
			Individual in = new Individual(2, rand, currentID, duplicate);
			//System.out.println(i);
			in.calculateCost();
			group.add(in);
			size++;
		}
		
		//three colfams
		for(int i = 0; i < Constants.NUM_OriginPopulationSize/2; i++){
			currentID ++;
			Individual in = new Individual(3, rand, currentID, duplicate);
			//System.out.println(i);
			in.calculateCost();
			group.add(in);
			size++;
		}
		
	}
	
	public void setRate(Environment e){
		this.copulationRate = e.copulationRate;
		this.crossOverRate = e.crossOverRate;
		this.crossOver2Rate = e.crossOver2Rate;
//		this.evolutionRate = e.evolutionRate;
		this.mutationRate = e.mutationRate;
		this.splitUpRate = e.splitUpRate;
		this.mergeRate = e.mergeRate;		
	}
	
	
	public void evolution() throws IOException{		
		time ++;
		int num = (int) (evolutionRate * size);	
		int num2 = (int) (copulationRate * size);
		
		for(int i = 0; i < num2; i ++){
			currentID ++;
			Individual son = this.copulate(group.get(rand.nextInt(this.size)), group.get(rand.nextInt(this.size)), currentID);
			Constants.eLog.printLogln("----------Copulate- new individual: " + son.id+"---------");
			son.printEvolutionLog(i);
			group.add(son);
			size++;
		}
		
		for (int i = 0; i < num; i++){
			currentID ++;

			//System.out.println("---No."+i+"----");
			Constants.eLog.printLogln("---No."+i+"----");
			int chooseID = rand.nextInt(size);
			Individual variant = new Individual(group.get(chooseID),currentID);
			
			//System.out.println("-------individual parent: " + group.get(chooseID).id + "-------");
			Constants.eLog.printLogln("-------individual parent: " + group.get(chooseID).id + "-------");
			//variant.printIndividual();
			variant.printEvolutionLog(i);
			//System.out.println("----------Actions---------");
			Constants.eLog.printLogln("----------Actions---------");

			//for each colFam do mutation (addition and remove)  with the Mutation Probability
			for(int j = 0; j < variant.colfams.size(); j++){
				ColFam c = variant.colfams.get(j);
				if(rand.nextFloat() < mutationRate){
					int times = rand.nextInt(mutationMaxAttNum)+1;
					//addition
					if(rand.nextFloat() < getAdditionProbability(c.getAttsNum())){
						for(int m = 0 ; m < times; m++){
							int attid = rand.nextInt(attSize);
							Attribute a = new Attribute(attid);
							if(c.addAtt(a)){
								//System.out.println("mutation ADD happened! " + j);
								Constants.eLog.printLogln("mutation ADD happened! " + j);

							}
						}
					}				
					//remove
					else{
						for(int m = 0 ; m < times; m++){
							int attid = rand.nextInt(attSize);
							Attribute a = new Attribute(attid);
							if(variant.isDuplicate(attid) && c.getAttsNum() > 1){
								if(c.removeAtt(a)){
									//System.out.println("mutation REMOVE happened! " + j);
									Constants.eLog.printLogln("mutation REMOVE happened! " + j);
								}
							}
						}
					}
				}
			}
			
			boolean varFlag = false;
			
			//1-point crossover
			if(rand.nextFloat() < crossOverRate){
				int cf1 = rand.nextInt(variant.colFamNum);
				int cf2 = rand.nextInt(variant.colFamNum);
				int pos1 = rand.nextInt(variant.colfams.get(cf1).getAttsNum());
				int pos2 = rand.nextInt(variant.colfams.get(cf2).getAttsNum());
				varFlag = variant.crossOver(cf1, cf2, pos1, pos2);
				if(varFlag){
					//System.out.println("crossover happened! " + cf1+" " + cf2+" " + pos1+" " + pos2);
					Constants.eLog.printLogln("crossover happened! " + cf1+" " + cf2+" " + pos1+" " + pos2);
				}
			}
			
			//2-points crossover
			else if(rand.nextFloat() < crossOver2Rate){
				int cf1 = rand.nextInt(variant.colFamNum);
				int cf2 = rand.nextInt(variant.colFamNum);
				int pos11 = 0, pos12, pos21 = 0,pos22 = 0; 
				if(variant.colfams.get(cf1).getAttsNum()/2 > 3 && variant.colfams.get(cf2).getAttsNum()/2 >3){
					pos11 = rand.nextInt(variant.colfams.get(cf1).getAttsNum()/2-3) + 1;
					pos21 = rand.nextInt(variant.colfams.get(cf2).getAttsNum()/2-3) + 1;
					pos12 = rand.nextInt(variant.colfams.get(cf1).getAttsNum()/2-3) + 1;
					pos22 = rand.nextInt(variant.colfams.get(cf2).getAttsNum()/2-3) + 1;
					varFlag = variant.crossOver2(cf1, cf2, pos11, pos12, pos21, pos22);
				}
				else{
					varFlag = false;
				}
				
				if(varFlag){
					//System.out.println("2-point crossover happened! " + cf1+" " + cf2+" " + pos11+" " + pos21 + " " + pos22);
					Constants.eLog.printLogln("2-point crossover happened! " + cf1+" " + cf2+" " + pos11+" " + pos21 + " " + pos22);
				}
				
			}
			
			//split up
			if(rand.nextFloat() < splitUpRate){
				int cf = rand.nextInt(variant.colFamNum);
				int pos = rand.nextInt(variant.colfams.get(cf).getAttsNum());
				if(pos!=0){
					varFlag = variant.splitUp(cf, pos);
				}
				if(varFlag){
					//System.out.println("split up happened! " + cf +" "+ pos);
					Constants.eLog.printLogln("split up happened! " + cf +" "+ pos);
				}
			}
			
			//merge
			if(rand.nextFloat() < mergeRate){
				int cf1 = rand.nextInt(variant.colFamNum);
				int cf2 = rand.nextInt(variant.colFamNum);
				varFlag = variant.merge(cf1, cf2);
				if(varFlag){
					//System.out.println("merge happened! " + cf1 +" "+ cf2);
					Constants.eLog.printLogln("merge happened! " + cf1 +" "+ cf2);
				}
			}
			
			variant.calculateCost();
			
			//System.out.println("----------new individual: " + variant.id+"---------");
			Constants.eLog.printLogln("----------new individual: " + variant.id+"---------");
			//variant.printIndividual();
			variant.printEvolutionLog(i);
			group.add(variant);
			size++;

		}
		//System.out.println("Population: " + size);	
			

	}
	
	
	
	public void natureSelection(){
		float []probs = new float [size];
		float []randProbs = new float[size];
		float sumProb = 0;
		for(int i = 0; i < size; i++){
			probs[i] = (float)Math.exp(-Constants.FACTOR_Alpha *group.get(i).cost);
			sumProb += probs[i];
		}
		for(int i = 0; i < size; i++){
			probs[i] /= sumProb; //normalization
			randProbs[i] = rand.nextFloat() * probs[i];
			//randProbs[i] =  probs[i];
		}
		
		ArrayList<Float> surviveFactors = new ArrayList<Float>();
		
		for(int i = 0;i < size; i++){
			surviveFactors.add( randProbs[i]);
		}	
		
		while(size > Constants.NUM_OriginPopulationSize){
			//delete the minimum
			float minProb = 0;
			int minPos = 0;
			for(int i = 0; i < size; i++){
				if(i ==0 ){
					minPos = 0;
					minProb = surviveFactors.get(0);
				}
				else{
					if(minProb > surviveFactors.get(i)){
						minProb = surviveFactors.get(i);
						minPos = i;
					}
				}
			}
			group.remove(minPos);
			size--;
			surviveFactors.remove(minPos);
		}
		
		updateCost();
	}	
	
	public void natureSelection_eRANKING(float m){
		float []fitness = new float[size];
		float []probs = new float [size];
		float []randProbs = new float[size];
		float sumRank = 0;
		for(int i = 0; i < size; i++){
			fitness[i] = (float)Math.exp(-Constants.FACTOR_Alpha *group.get(i).cost);
		}
		
		int rankIndex[] = new int [size];
		//rank individuals
		for(int i = 0; i < size; i++){
			int maxIndex = -1;
			float maxFitness = 0;
			for(int j = 0; j < size; j ++){
				if(rankIndex[j] != 0){
					continue;
				}else{
					if(fitness[j] > maxFitness){
						maxFitness = fitness[j];
						maxIndex = j;
					}
				}	
			}
			rankIndex[maxIndex] = size-i;
			sumRank += Math.pow((size-i),m);
		}
		
		for(int i = 0; i < size; i++){
			probs[i] = (float) (Math.pow(rankIndex[i],m) / sumRank); //normalization
			randProbs[i] = rand.nextFloat() * probs[i];
			//randProbs[i] =  probs[i];
		}
		
		ArrayList<Float> surviveFactors = new ArrayList<Float>();
		
		for(int i = 0;i < size; i++){
			surviveFactors.add( randProbs[i]);
		}	
		
		while(size > Constants.NUM_OriginPopulationSize){
			//delete the minimum
			float minProb = 0;
			int minPos = 0;
			for(int i = 0; i < size; i++){
				if(i ==0 ){
					minPos = 0;
					minProb = surviveFactors.get(0);
				}
				else{
					if(minProb > surviveFactors.get(i)){
						minProb = surviveFactors.get(i);
						minPos = i;
					}
				}
			}
			group.remove(minPos);
			size--;
			surviveFactors.remove(minPos);
		}
		
		updateCost();
	}	
	
	
	public void natureSelection_Roulette_eRank(float m){
		float []fitness = new float[size];
		float rankIndex[] = new float [size];
		float sumRank = 0;
		for(int i = 0; i < size; i++){
			fitness[i] = (float)Math.exp(-Constants.FACTOR_Alpha *group.get(i).cost);
		}
		
		//rank individuals
		for(int i = 0; i < size; i++){
			int maxIndex = -1;
			float maxFitness = 0;
			for(int j = 0; j < size; j ++){
				if(rankIndex[j] != 0){
					continue;
				}else{
					if(fitness[j] > maxFitness){
						maxFitness = fitness[j];
						maxIndex = j;
					}
				}	
			}
			rankIndex[maxIndex] = (float) (Math.pow(size-i,m));
			sumRank += Math.pow((size-i),m);
		}
		
		ArrayList<Individual> newgroup = new ArrayList<Individual> ();
		
		//Roulette Wheel Selection
		for(int i = 0; i < Constants.NUM_OriginPopulationSize; i++){
			float pointer = rand.nextFloat() * sumRank;
			float s = 0;
			for(int j = 0; j < size; j++){
				s += rankIndex[j];
				if(pointer < s ){
					newgroup.add(new Individual(group.get(j),group.get(j).id));
					break;
				}
			}
		}
		
		group = newgroup;
		size = group.size();
		
		updateCost();
	}	
	
	public Individual copulate(Individual mama, Individual papa, int id){
		Individual son = new Individual(id);
		int m1 = mama.colFamNum/2 + mama.colFamNum%2 * rand.nextInt(2);
		int m2 = papa.colFamNum/2 + papa.colFamNum%2 * rand.nextInt(2);
		
		while( (m1+m2) > Constants.colFamNum_MAX){
			m2 --;
		}
		
		son.cfLoaded = new float [m1+m2];

		for(int i = 0; i < m1; i++){
			ColFam colfam = new ColFam(mama.colfams.get(i));
			son.colfams.add(colfam);
			son.colFamNum++;
		}
		
		int count = 0;
		for(int i = 0; i < m2; i++){
			ColFam colfam = new ColFam(papa.colfams.get(papa.colFamNum/2 + i));
			for(int j = 0; j < colfam.getAttsNum(); j++){
				if(son.isExist(colfam.getAttByIndex(j).getID())){
					colfam.removeAttByIndex(j);
					j--;
				}
			}
			if(colfam.getAttsNum()!=0){
				son.colfams.add(colfam);
				son.colFamNum++;
				count++;
			}

		}
		if(count!=0){
			son.fillUp(rand, son.colFamNum - count, son.colFamNum);
		}
		else{
			son.fillUp(rand, 0, son.colFamNum);
		}
		son.calculateCost();
		
		return son;
	
	}
	
	public Individual getBestIndividual(){
		int minIndex = 0;
		for(int i = 0; i < group.size(); i ++){
			if(group.get(i).cost < group.get(minIndex).cost){
				minIndex = i;
			}
		}
		return group.get(minIndex);
	}
	
	public void display() throws IOException{
		int count = 0;
		for(Individual i : this.group){
			count++;
			System.out.println("-----------Individual: " + count +" -----------------");
			i.printIndividual();			
			i.printLog(count);			
		}
		count = 0;
		float mincost = 0;
		int min = 0;
		for(Individual i : this.group){
			if(count != 0){
				if(mincost > i.cost){
					mincost = i.cost;
					min = count;
				}
			}
			else{
				mincost = i.cost;	
			}
			count ++;
		}
		group.get(min).printLog(min+1);
	}
	

	public void printLog_parameters() throws IOException {
		Log l = new Log(Constants.LOG_FILE_NAME);
		l.start(false);
		l.printLogln("-----------Experiment Parameters-----------------");
//		l.printLogln("Source Queries File: " + Constants.Query_FILE_NAME);
//		l.printLogln("Iterations: " + Constants.ITERATION);
		l.printLogln("colFamNum_MAX: " + Constants.colFamNum_MAX);
		l.printLogln("NUM_MutationMaxAttNum: " + Constants.NUM_MutationMaxAttNum);
		l.printLogln("NUM_AttributeSize: " + Constants.NUM_AttributeSize);
		l.printLogln("NUM_OriginPopulationSize: " + Constants.NUM_OriginPopulationSize);
		l.printLogln("NUM_Queries: " + DataQueries.queries.size());
		l.printLogln("RATE_copulationRate: " + Constants.RATE_CopulateRate);
		l.printLogln("RATE_evolutionRate: " + Constants.RATE_evolutionRate);
		l.printLogln("RATE_mutationRate: " + Constants.RATE_mutationRate);
		l.printLogln("RATE_crossOverRate: " + Constants.RATE_crossOverRate);
		l.printLogln("RATE_crossOver2Rate: " + Constants.RATE_crossOver2Rate);
		l.printLogln("RATE_mergeRate: " + Constants.RATE_mergeRate);
		l.printLogln("RATE_splitUpRate: " + Constants.RATE_splitUpRate);
		l.printLogln("FACTOR_Alpha: " + Constants.FACTOR_Alpha);
		l.printLogln("FACTOR_ColFamConsume: " + Constants.FACTOR_ColFamConsume);
		l.printLogln("FACTOR_ColFamNum: " + Constants.FACTOR_ColFamNum);
		l.printLogln("FACTOR_Duplicate: " + Constants.FACTOR_Duplicate);
		l.printLogln("FACTOR_Skew: " + Constants.FACTOR_Skew);
		l.printLogln("FACTOR_LoadBalance: " + Constants.FACTOR_LoadBalance);
		l.end();
	}
	
	
	private float getAdditionProbability(int attNum){
		return (float)(attSize - attNum)/attSize;
	}
	
	
	private void updateCost(){
		float tempCost = 0;
		float tempSkew = 0;
		float tempColFamConsume = 0;
		int tempSumAtts = 0;
		float tempLoadBalance = 0;
		float tempDuplicateRate = 0;
		for(Individual i : group){
			tempCost += i.cost;
			tempSkew += i.skew;
			tempColFamConsume += i.ColFamConsume;
			tempSumAtts += i.sumAtts;
			tempLoadBalance += i.loadBalance;
			tempDuplicateRate += i.duplicateRate;
		}
		this.avgCost = tempCost/group.size();
		this.avgColFamConsume = tempColFamConsume/group.size();
		this.avgSkew = tempSkew / group.size();
		this.avgSumAtts = tempSumAtts / group.size();		
		this.avgLoadBalance = tempLoadBalance / group.size();
		this.avgDuplicateRate = tempDuplicateRate / group.size();
	}	
}