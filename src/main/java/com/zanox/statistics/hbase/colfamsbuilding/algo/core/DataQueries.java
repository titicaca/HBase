package com.zanox.statistics.hbase.colfamsbuilding.algo.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.zanox.statistics.hbase.colfamsbuilding.algo.types.Attribute;
import com.zanox.statistics.hbase.colfamsbuilding.algo.types.Query;

public class DataQueries{
	public static List<Query> queries = new ArrayList<Query> ();
	
	public static void initQueriesFromFile(String filename, String splitSymbol) throws IOException{
		queries.clear();
		File file = new File(filename);
		BufferedReader br = new BufferedReader(new FileReader(file));

		while(br.ready()){
			String line = br.readLine();
			String []items = line.split(splitSymbol);
			int []m = new int [items.length];
			Query q = new Query();
			for(int i = 0 ; i < items.length; i++){
				m[i] = Integer.parseInt(items[i]);
				if(m[i] == 1){
					q.addAtt(new Attribute(i));
				}
			}
			queries.add(q);
		}		
	}
	
	public static void initQueryPrototypesFromFile(String filename, String splitSymbol) throws IOException{
		queries.clear();
		File file = new File(filename);
		BufferedReader br = new BufferedReader(new FileReader(file));
		int sum = 0;
		br.readLine();//jump the first info line
		while(br.ready()){
			String line = br.readLine();
			String []items = line.split(splitSymbol);
			int []m = new int [items.length];
			Query q = new Query();
			for(int i = 0 ; i < items.length; i++){
				if(i < items.length - 1){
					m[i] = Integer.parseInt(items[i]);
					if(m[i] == 1){
						q.addAtt(new Attribute(i));
					}
				}
				else{
					q.frequency = Integer.parseInt(items[i]);
					sum += Integer.parseInt(items[i]);
				}
				
			}
			queries.add(q);
		}
		
		for(int i = 0; i < queries.size(); i++){
			queries.get(i).frequency /= sum;
		}
	}
	
	static public void printQueries(){
		for(Query q : queries){
			q.printQuery();
		}
	}
}