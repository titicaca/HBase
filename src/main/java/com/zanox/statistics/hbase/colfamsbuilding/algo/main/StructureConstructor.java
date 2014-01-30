package com.zanox.statistics.hbase.colfamsbuilding.algo.main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.zanox.statistics.hbase.colfamsbuilding.algo.core.Constants;



public class StructureConstructor{
	
	int structuresNum = 1;
	
	public StructureConstructor(int structuresNum){
		this.structuresNum = structuresNum;
	}
	
	public void structure00 (String outputFile) throws IOException{
		System.out.println("Building Column Family (one Column Family) ...");
		File file = new File (outputFile);
		BufferedWriter bw = new BufferedWriter (new FileWriter(file,false));
		
		bw.write("1\n");
		
		String items[] = Constants.attributes.split(", ");
		
		for(int i = 0; i <items.length; i ++ ){
			bw.write(i+" ");
		}
		bw.write("\n");
		bw.close();
		System.out.println("Done! Output Path: " + outputFile);

	}
	
	public void structure03 (String outputFile) throws IOException{
		System.out.println("Building Column Family (logical division) ...");

		File file = new File(outputFile);
		BufferedWriter bw = new BufferedWriter (new FileWriter(file, false));
		
		bw.write(this.structuresNum + "\n");
		
		
		String items[] = Constants.attributes.split(", ");

		for(int i = 0; i <items.length; i ++ ){
			if(items[i].contains("PPS")){
				bw.write(i+" ");
			}
		}
		bw.write("\n");
		
		for(int i = 0; i < items.length; i ++){
			 if(items[i].contains("PPL")){
				 bw.write(i + " ");
			 }
		}
		bw.write("\n");
		
		for(int i = 0; i <items.length; i ++ ){
			if(items[i].contains("PPC") || (!items[i].contains("PPC") && !items[i].contains("PPS") && !items[i].contains("PPL") && !items[i].contains("TPV")&& !items[i].contains("PPV") )){
				bw.write(i+" ");
			}
		}
		bw.write("\n");
		
		for(int i = 0; i <items.length; i ++ ){
			if(items[i].contains("TPV") || items[i].contains("PPV")){
				bw.write(i+" ");
			}
		}
		bw.write("\n");				
		bw.close();
		System.out.println("Done! Output Path: " + outputFile);		

	}
	
	public static void main (String []args){
		StructureConstructor sc = new StructureConstructor(4);
		try {
			sc.structure00("results//structures//s00");
			sc.structure03("results//structures//s03");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		
		
	}
	
}