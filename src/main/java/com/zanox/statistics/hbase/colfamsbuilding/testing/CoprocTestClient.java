package com.zanox.statistics.hbase.colfamsbuilding.testing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;

import com.zanox.statistics.hbase.colfamsbuilding.algo.types.ColFam;
import com.zanox.statistics.hbase.colfamsbuilding.algo.types.Query;
import com.zanox.statistics.hbase.colfamsbuilding.coprocs.ColumnValueCountProtocol;
import com.zanox.statistics.hbase.colfamsbuilding.importer.Constants;
import com.zanox.statistics.hbase.colfamsbuilding.importer.Rowkey;



public class CoprocTestClient{
	private static Configuration conf = null;
	static {
	     conf = HBaseConfiguration.create();
	     conf.set("hbase.zookeeper.quorum", "t-cloudera-01.zanox.com");
	}
	
	private static HTablePool pool = null;
	
	private static Integer bulkSize = 2097152;
	
	private static HTableInterface htable = null;
	
	ArrayList<ColFam> colfams = new ArrayList<ColFam> ();
	
	int iteration;
	
	CoprocTestClient(int i, int structures){
		this.iteration = i;
		structureNum = structures;
		queryTime = new long[structureNum][iteration];
		
	}
	
	int queryNum = 0;
		
	String queryFileName = "bigQueries";
	
	ArrayList<Query> queries = new ArrayList<Query>();
	
	int getGroupNum = 1;
	
	int structureNum;
	long queryTime[][] = null;
	
	
	long sendQueries(String tableName, int randomSeed, int structureNO) throws Throwable{
		File file = new File(queryFileName);
		Random r = new Random(randomSeed);

		BufferedReader br = new BufferedReader ( new FileReader(file));
		br.readLine();
		
		pool = new HTablePool(conf, 10);
		htable = pool.getTable(tableName);
		htable.setAutoFlush(false);
		htable.setWriteBufferSize(bulkSize);
		
		long responseTime = 0;
		long startTime;
		long endTime;
		//long startTime = System.nanoTime();
		for(int m = 0; m < iteration && br.ready(); m++ ){
			
			String line = br.readLine();
			String items[] = line.split(",");
			
			int progId = r.nextInt(Constants.MAX_NUM_PROGS);
			String startDate = "2011-01-01";
			String endDate = "20113-01-01";

			
			Query q = new Query (items, false);
			
			final int size = q.getAttNum();
			final byte[][] columnFamily = new byte[size][Bytes.toBytes("cf1").length];
			final byte[] startRow = new Rowkey(progId,startDate).getBytes();
			final byte[] endRow = new Rowkey(progId,endDate).getBytes();
			final byte[][] qualifier = new byte[size][Bytes.toBytes((int)0).length];

			setQueryParameters(columnFamily,qualifier, q);
			
			startTime = System.nanoTime();
	    	
	    	Map<byte[], double[]> results = htable.coprocessorExec(ColumnValueCountProtocol.class,
	    			startRow,
	    			endRow,
	    			new Batch.Call<ColumnValueCountProtocol, double[]>(){
	    				public double[] call(ColumnValueCountProtocol counter) throws IOException{

							return counter.getValue(startRow, endRow, columnFamily, qualifier, size);
	    					//return counter.getRowCount(filter1);
	    				}
	    			}
	    		);
			
 
			endTime = System.nanoTime();
			responseTime += (endTime - startTime);
			queryTime[structureNO][m] = (endTime - startTime)/1000;				
		}	
		//long endTime = System.nanoTime();
		//responseTime = (endTime - startTime);
		htable.close();
		br.close();
		return responseTime;
	}
	
	private boolean setQueryParameters(byte[][] columnFamily, byte[][] qualifier, Query q){
		Query copy = new Query(q);
		int index = 0;
		while(copy.getAttNum()>0){				
			//choose the colfam which has the max matched atts
			int maxMatchedAtts = 0;
			int maxCf = 0;
			int matchedAtts = 0;
			for(int i = 0; i < colfams.size(); i++){
				ColFam c = colfams.get(i);
				for(int x = 0; x < copy.getAttNum(); x++ ){
					for(int y = 0; y < c.getAttsNum(); y++){
						if(copy.getAttByIndex(x).equals( c.getAttByIndex(y))){
							matchedAtts ++;
							break;
						}
					}
				}					
				if(i == 0){
					maxMatchedAtts = matchedAtts;
					maxCf = 0;
				}
				else{
					if(maxMatchedAtts < matchedAtts){
						maxCf = i;
						maxMatchedAtts = matchedAtts;
					}
				}					
			}
			if(maxMatchedAtts == 0 )
			{
				System.out.println("Error Happened!");
				return false;
			}
			
			//delete the matched atts in the query, add query to get()
			ColFam c = colfams.get(maxCf);
			for(int y = 0; y < c.getAttsNum(); y++){
				for(int x = 0; x < copy.getAttNum(); x++ ){
					if(copy.getAttByIndex(x).equals(c.getAttByIndex(y))){
						index = q.getFirstMatchedAttIndex(copy.getAttByIndex(x).getID());
						columnFamily[index] = Bytes.toBytes(c.getName());
						qualifier[index] = Bytes.toBytes(copy.getAttByIndex(x).getID());
						copy.removeAttByIndex(x);
//						System.out.println("q current size: " + q.getAttNum() + "  index: " + index);
						break;
					}
				}
			}
		}
		return true;
	}
	
	void loadColFams(String structureFile) throws IOException{
		File file = new File(structureFile);
		BufferedReader br = new BufferedReader (new FileReader(file));
		int num = Integer.parseInt(br.readLine());
		int no = 0;
		colfams.clear();
		while(br.ready()){
			no++;
			String line = br.readLine();
			String items[] = line.split(" ");
			ColFam c = new ColFam(items, ("cf"+no) );
			colfams.add(c);	
		}
		br.close();
	}
	
	void writeQueryTime(String resultFile) throws IOException{
		File file = new File(resultFile);
		BufferedWriter bw = new BufferedWriter(new FileWriter(file, false));
		for(int i = 0; i < iteration; i++){
			String line = new String();
			for(int j = 0; j < structureNum; j++){
				line += queryTime[j][i];
				if(j != structureNum - 1){
					line += " ";
				}else{
					line += "\n";
				}
			}
			bw.write(line);
		}
		bw.close();
	}
	
	public static void main(String args[]) throws Throwable{

		CoprocTestClient client = new CoprocTestClient(1000,4);
		long time;
		System.out.println("Iteration: " + client.iteration);
		
		client.loadColFams("structures//Structure00");
		time = client.sendQueries("booking_00", 1000, 0) / 1000 /1000;
		System.out.println("Response Time 0: " + time + " ms");
		
		client.loadColFams("structures//Structure01");
		time = client.sendQueries("booking_01", 1000, 1) / 1000 /1000;
		System.out.println("Response Time 1: " + time + " ms");
		
		client.loadColFams("structures//Structure02");
		time = client.sendQueries("booking_02", 1000, 2) / 1000 /1000;
		System.out.println("Response Time 2: " + time + " ms");
		
		client.loadColFams("structures//Structure03");
		time = client.sendQueries("booking_03", 1000, 3) / 1000 /1000;
		System.out.println("Response Time 3: " + time + " ms");
		
		client.pool.close();
		client.writeQueryTime("results_s1000_2year");
	}
}