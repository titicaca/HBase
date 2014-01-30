package com.zanox.statistics.hbase.colfamsbuilding.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class LargeFileReader{
	public String intputFileName;
	public String ouputFileName;
	
	LargeFileReader(String file, String output){
		this.intputFileName = file;
		this.ouputFileName = output;
	}
	
	public int readFile(int startLineNo, int endLineNo) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(this.intputFileName));
		BufferedWriter bw = new BufferedWriter(new FileWriter(this.ouputFileName,false));
		
		int linecount = 0;
		int lineno = 0;
		boolean flag = true;

		while(flag && br.ready()){
			lineno ++;

			if(lineno >= startLineNo){
				
				if(lineno >= endLineNo){
					flag = false;
					break;
				}
				
				linecount ++;
				String line = br.readLine();
				bw.write(line + "\n");
//				System.out.println(line);
			}
		}
		bw.flush();
		bw.close();
		br.close();
		return linecount;
		
	}
	
	public static int countRowNums(String fileName) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		int lineno = 0;
		while(br.ready()){

			lineno ++;
			br.readLine();
		}
		return lineno;
	}
	
	public static void main(String [] args) throws IOException{
		LargeFileReader reader = new LargeFileReader("datasets//LearningDatasets//Training_Query", "TmpReader");
		int count = reader.readFile(0, 1000);
		
//		int count = countRowNums("dataset");
//		System.out.print(count);
	}
}