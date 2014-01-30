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
 * Get prototypes from the filtered query file
 * @author fangzhou.yang
 *
 */
public class QueryAnalyzer{
	
	public static void main(String args[]) throws IOException{

		String queryFile = "datasets\\QueryDataset\\FilteredQueries";
		
		File file = new File(queryFile);
		BufferedReader br = new BufferedReader (new FileReader(file));
		
		File outputfile = new File("datasets\\QueryDataset\\FilteredQueries_ProtoTypes");

		BufferedWriter bw = new BufferedWriter (new FileWriter(outputfile, false));
		
		ArrayList<String> prototypes = new ArrayList<String>();
		boolean flag = false;
		int rowcount = 0;

		while(br.ready()){
			rowcount++;
			flag = false;
			String line = br.readLine();
			String queryColumns  = new String();
			
			if(prototypes.size() == 0){
				queryColumns  = new String();
				String items [] = line.split(",");
				for (int i = 0; i < 109; i ++){
					queryColumns += items[i];
					if(i != 108){
						queryColumns += ",";						
					}
				}
			}

			for(String s: prototypes){
				queryColumns  = new String();
				String items [] = line.split(",");
				for (int i = 0; i < 109; i ++){
					queryColumns += items[i];
					if(i != 108){
						queryColumns += ",";						
					}
				}
				if(s.compareTo(queryColumns)==0){
					flag = true;
					break;
				}
			}
			if(!flag){
				prototypes.add(queryColumns);
				System.out.println("rowcount: " + rowcount + " add prototype..\n" + queryColumns + "\ncurrent protortpe #: " + prototypes.size());

			}
		}
		br.close();
		
		rowcount = 0;
		int counter[] = new int [prototypes.size()];
		br = new BufferedReader (new FileReader(file));
		while(br.ready()){
			rowcount ++;
			flag = false;
			String line = br.readLine();
			String queryColumns  = new String();
			String items [] = line.split(",");
			for (int i = 0; i < 109; i ++){
				queryColumns += items[i];
				if(i != 108){
					queryColumns += ",";						
				}
			}
			for(int i = 0; i < prototypes.size(); i++){
				if(prototypes.get(i).compareTo(queryColumns)==0){
					counter[i]++;
					break;
				}
			}
		}
		br.close();
		
		System.out.println("# Prototypes: " + prototypes.size());
		bw.write("# Prototypes: " + prototypes.size() + "\n");
		for(int i = 0; i < prototypes.size(); i++){
			if(prototypes.get(i) == null) 
				continue;
			System.out.print(prototypes.get(i));
			System.out.print(",");
			System.out.println(counter[i]);
			bw.write(prototypes.get(i) + "," + counter[i] +"\n");
		}	
		bw.close();
		br.close();
		System.out.println(rowcount);
	}
}