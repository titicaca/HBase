package com.zanox.statistics.hbase.colfamsbuilding.algo.core;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


public class Log{
	String filename;
	String path = "logs/";
	File file;
	BufferedWriter bw;
	boolean flag = false;
	
	public Log(String s){
		filename = path + s;
	}
	
	public void start() throws IOException{
		flag = true;
		file = new File(filename);
		bw = new BufferedWriter(new FileWriter(file,true));
	}
	
	public void start(boolean b) throws IOException{
		flag = true;
		file = new File(filename);
		bw = new BufferedWriter(new FileWriter(file,b));
	};
	
	public void printLog(String line) throws IOException{
		if(flag){
			bw.write(line);		
		}
	}
	
	public void printLogln(String line) throws IOException{
		if(flag){
			bw.write(line+"\n");		
		}
	}
	
	public void end() throws IOException{
		flag = false;
		bw.close();
	}

	public void printLogln(int i) throws IOException {
		if(flag){
			bw.write(String.valueOf(i));
			bw.write("\n");

		}
	}

	public void printLog(int i) throws IOException {
		if(flag){
			bw.write(String.valueOf(i));
		}
	}

	public void printLogln(float f) throws IOException {
		if(flag){
			bw.write(String.valueOf(f));
			bw.write("\n");

		}
	}
	
	public void flush() throws IOException{
		if(flag){
			bw.flush();
		}
	}
}