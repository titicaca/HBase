package com.zanox.statistics.hbase.colfamsbuilding.algo.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;


public class AttWeights{
	
	float weight[] = new float[Constants.NUM_AttributeSize];
	
	public AttWeights(){
		for(int i = 0; i < Constants.NUM_AttributeSize; i ++){
			weight[i] = 1/Constants.NUM_AttributeSize;
		}
	}
	
	/*
	 * get weight for each att from the dataset file
	 */
	//lineNo starts from 1
	void getWeightsFromFile(String filename, String splitSymbol, int lineNo) throws IOException{
		File file = new File(filename);
		BufferedReader br = new BufferedReader(new FileReader(file));
		int count=0;
		while(br.ready()){
			String line = br.readLine();
			count++;
			if(count != lineNo){
				continue;
			}else{
				String items[] = line.split(splitSymbol);
				float sum = 0;
				for(int i = 0; i < items.length; i++){
					weight[i] = Float.parseFloat(items[i]);
					sum += weight[i];
				}
				for(int i = 0; i < items.length; i++){
					weight[i] /= sum;
				}
			}
		}
	}
	
	public float getWeightByAttId(int i){
		return weight[i];
	}
}