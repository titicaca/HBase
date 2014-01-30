package com.zanox.statistics.hbase.colfamsbuilding.datasets;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * This class is to calculate the weight for each attribute
 * Input dataFile
 * Output weightFile
 * @author fangzhou.yang
 *
 */
public class WeightsCalculation{
	
	public static String dataFilePath = WeightsCalculation.class.getResource("/StorageDataset/dataset").getPath();
	public static String weightFilePath ="datasets//weight";
	
			
	public static void main(String args[]) throws IOException{
		
		String []atts = Constants.attributes.split(", ");
		
		
		int counter[] = new int [atts.length];
		
		File dataFile = new File( dataFilePath );
		BufferedReader br = new BufferedReader (new FileReader(dataFile));
		
		File weightFile = new File(weightFilePath);
		BufferedWriter bw = new BufferedWriter(new FileWriter(weightFile,false));
		
		String line = new String();
		int lineNo = 0;
		while(br.ready()){
			
			lineNo++;
			
			
			br.read();
			line = br.readLine();
			
			if(lineNo % 10000 ==0){
				System.out.println("Line: " + lineNo);
				System.out.println(line);
			}

			
			String items[] = line.split(",", -1);
			

			
			for(int i = 0; i < items.length; i++){
				if(items[i].compareTo("")!=0){
					counter[i] ++;
				}
			}
			
			
			//if(lineNo>5000) break;
			
		}
		
		
		String count = new String ();
		for(int i = 0; i < counter.length; i++){
			count = count + counter[i];
			if(i == counter.length-1){
				count += "\n";
			}else{
				count +=",";
			}
		}
		bw.write(count);
		
		br.close();
		bw.close();
		
		
	}
	
}