package com.zanox.statistics.hbase.colfamsbuilding.algo.types;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;


public class Query{
	ArrayList <Attribute> atts = new ArrayList<Attribute>();
	public float frequency = 1;
	
	//construct from the file, items is 0/1 bit string
	public Query(String items[], boolean isPrototype){
		if(isPrototype){
			for( int i = 0; i < items.length; i++){
				if(i == items.length-1){
					this.frequency = Integer.parseInt(items[i]);	
				}
				else{
					if(items[i].compareTo("1") == 0){
						atts.add(new Attribute(i));
					}
				}
			}
		}else{
			for( int i = 0; i < items.length; i++){
				if(items[i].compareTo("1") == 0){
					atts.add(new Attribute(i));
				}
			}
		}		
	}
	
	public Query(int items[]){
		for( int i = 0; i < items.length; i++){
			atts.add(new Attribute(items[i]));
		}
	}
	
	public Query(){
		
	}
	public Query(Query q){
		atts = new ArrayList<Attribute>();
		for(Attribute a: q.atts){
			atts.add(a);
		}
		frequency = q.frequency;
	}
	
	public int getAttNum(){
		return atts.size();
	}
	
	public int getFirstMatchedAttIndex(int attid){
		for(int i = 0; i < atts.size(); i ++){
			Attribute a = atts.get(i);
			if(a.id == attid){
				return i;
			}
		}
		return -1;
	}
	
	public Attribute getAttByIndex(int i){
		return this.atts.get(i);
	}
	
	public void addAtt(Attribute a){
		this.atts.add(a);
	}
	
	public boolean removeAttByIndex (int i){
		if(i < this.atts.size()){
			this.atts.remove(i);	
			return true;
		}
		else{
			return false;
		}
	}
	
	public void printQuery(){
		for(Attribute a: atts){
			System.out.print(a.id + " ");
		}
		System.out.print(this.frequency);
		System.out.print("\n");
	}
	
	@Override
	public String toString(){
		String s = new String();
		s += "[";
		for(Attribute a: atts){
			s += a.id + " ";
		}
		s += "]";
		return s;
	}
	
	public static ArrayList<Query> getQueriesFromFile(String filename, String splitSymbol) throws IOException{
		File file = new File(filename);
		BufferedReader br = new BufferedReader(new FileReader(file));
		ArrayList<Query> queries = new ArrayList<Query>();

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
		return queries;
	}
	
	public static ArrayList<Query> getQueryPrototypesFromFile(String filename, String splitSymbol) throws IOException{
		File file = new File(filename);
		BufferedReader br = new BufferedReader(new FileReader(file));
		ArrayList<Query> queries = new ArrayList<Query>();
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
		return queries;
	}
}