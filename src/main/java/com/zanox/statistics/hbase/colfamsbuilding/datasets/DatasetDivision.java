package com.zanox.statistics.hbase.colfamsbuilding.datasets;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * this class divide a user-log file into three query prototypes file as sequence according to given rates.
 * Three files: one for training, one for validation, one for testing
 * each query prototypes file contains query prototypes and corresponding frequency
 * Input: queries Dataset
 * Output 3 query prototype files, named "Training","Validation", "Testing" in the destPath
 * @author fangzhou.yang
 *
 */
public class DatasetDivision{
	private String fileLocate;
	
	private double trainingRate;
	private double validationRate;
	private double testRate;
	private int lineCount;
	
	private String destPath = "datasets//LearningDatasets//";
	
	/*
	 * the sum of three rates should be 1.0
	 */
	public DatasetDivision(String path, double trainrate, double validationrate, double testrate){
		fileLocate = path;
		this.trainingRate = trainrate;
		this.validationRate = validationrate;
		this.testRate = testrate;
		lineCount = this.getLineCount();
	}
	
	public void changeDefaultDestPath(String path){
		destPath = path;
	}
	
	
	public void divideQueryDatasets() throws IOException{
		String trainQueries = destPath + "Training_Query";
		String testQueries = destPath + "Testing_Query";
		BufferedReader br = new BufferedReader(new FileReader(fileLocate));
		int currentLineNo = 0;
		
		BufferedWriter bwTrain = new BufferedWriter (new FileWriter(trainQueries, false));
		BufferedWriter bwTest = new BufferedWriter (new FileWriter(testQueries, false));
		
		while(br.ready()){
			currentLineNo ++;
			String line = br.readLine();
			line += "\n";
			if(currentLineNo % 10000 == 0){
				System.out.println("scanning lines:" + currentLineNo );
//				System.out.println(line);
			}
			
			if(currentLineNo <= lineCount * trainingRate){
				bwTrain.write(line);
			}else {
				bwTest.write(line);
			}			
		}
		
		bwTrain.close();
		bwTest.close();
		
	}
	
	/**
	 * Generate training validation and test dataset for query prototypes
	 * @throws IOException 
	 */
	public void generateProtoTypesSets() throws IOException{
		String trainingFile = destPath + "Training";
		String validationFile = destPath + "Validate";
		String testFile = destPath + "Testing";
		
		BufferedReader br = new BufferedReader(new FileReader(fileLocate));
		int currentLineNo = 0;
		
		BufferedWriter bwTrain = new BufferedWriter (new FileWriter(trainingFile, false));
		BufferedWriter bwValidation = new BufferedWriter (new FileWriter(validationFile, false));
		BufferedWriter bwTest = new BufferedWriter (new FileWriter(testFile, false));
		
		ArrayList<String> prototypesTrain = new ArrayList<String>();
		ArrayList<String> prototypesValidation = new ArrayList<String>();
		ArrayList<String> prototypesTest = new ArrayList<String>();
		
		int MAX_SIZE = lineCount;
		
		int counterTrain[] = new int [MAX_SIZE];
		int counterValidation[] = new int [MAX_SIZE];
		int counterTest[] = new int [MAX_SIZE];
		int counter[] = null;

		boolean flag = false;
		ArrayList<String> prototypes = null;
		
		System.out.println("Generating Prototypes ...");

		while(br.ready()){
			flag = false;
			String line = br.readLine();
			String items[] = line.split(",");
			String query = new String();
			for(int i = 0; i < 109; i ++){
				query += items[i];
				if(i == 108){
					
				}else{
					query += ",";
				}
			}
			currentLineNo ++;
			
			if(currentLineNo % 10000 == 0){
				System.out.println("scanning lines:" + currentLineNo + "  num prototypes: " + prototypes.size());
//				System.out.println(line);
			}
			
			if(currentLineNo <= lineCount * trainingRate){
				prototypes = prototypesTrain;
				counter = counterTrain;
			}else if(currentLineNo <= lineCount * (trainingRate + validationRate)){
				prototypes = prototypesValidation;
				counter = counterValidation;
			}else {
				prototypes = prototypesTest;
				counter = counterTest;
			}			
			for(int i = 0; i < prototypes.size(); i++){
				if( query.compareTo(prototypes.get(i)) == 0){
					flag = true;
					counter[i] ++;
					break;
				}
			}
			if(!flag){
				prototypes.add(query);
				counter[prototypes.size()-1] = 1;
			}
		}	
		br.close();
		
		System.out.println("Generating Prototypes done!");
		System.out.println("Train prototypes: " + prototypesTrain.size());
		System.out.println("Validation prototypes: " + prototypesValidation.size());
		System.out.println("Test prototypes: " + prototypesTest.size());
		
		System.out.println("Writing data into files..");

		bwTrain.write("# Training Prototypes: " + prototypesTrain.size() + "\n");
		for(int i = 0; i < prototypesTrain.size(); i++){
			bwTrain.write(prototypesTrain.get(i) + "," + counterTrain[i] +"\n");
		}	
		bwTrain.close();
		System.out.println("Writing data into Training done!");

		bwValidation.write("# Validation Prototypes: " + prototypesValidation.size() + "\n");
		for(int i = 0; i < prototypesValidation.size(); i++){
			bwValidation.write(prototypesValidation.get(i) + "," + counterValidation[i] +"\n");
		}	
		bwValidation.close();
		System.out.println("Writing data into Validation done!");

		bwTest.write("# Testing Prototypes: " + prototypesTest.size() + "\n");
		for(int i = 0; i < prototypesTest.size(); i++){
			bwTest.write(prototypesTest.get(i) + "," + counterTest[i] +"\n");
		}	
		bwTest.close();
		
		System.out.println("Writing data into Testing done!");

		
	}

	
	private int getLineCount(){
		int count = 0;
		try {
			BufferedReader br = new BufferedReader(new FileReader(fileLocate));
			while(br.ready()){
				br.readLine();
				count ++;
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return -1;
		} catch (IOException e) {
			e.printStackTrace();
			return -1;
		}		
		return count;
	}
	
	public static void main(String [] args) throws IOException{
		DatasetDivision dd = new DatasetDivision("datasets//QueryDataset//" + "FilteredQueries", 0.8, 0.1, 0.1);
		System.out.println("init class done! Line Count: " + dd.lineCount );

//		dd.generateProtoTypesSets();
		dd.divideQueryDatasets();
	}
	
	
}