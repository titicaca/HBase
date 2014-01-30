package com.zanox.statistics.hbase.colfamsbuilding.testing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.zanox.statistics.hbase.colfamsbuilding.algo.types.ColFam;
import com.zanox.statistics.hbase.colfamsbuilding.algo.types.Query;
import com.zanox.statistics.hbase.colfamsbuilding.importer.TypeRowKey;


public class ColFamTestWithQueryThesholds{
	

//	final int MAX_PROGID = 10000;
//	final int MAX_PROGID = 500;
	
	static int numProgs = 10000;
	static int numActiveProgs = 500;
	
	final String tableNames [];
	int tableTypes [];
	
	// query file extracted by extractTimeQuery() from class QueryLogExtractor
	final String queryFileName ;
	final String outputPath;
	final int thresholds[];
	long numQueries;
	final int numTables;
	final int numThresholds;
	
	SimpleDateFormat DATE_FORMAT_SHORT = new SimpleDateFormat("yyyyMMdd");
	SimpleDateFormat DATE_FORMAT_LONG = new SimpleDateFormat("yyyy-MM-dd");
	
	/**
	 * 
	 * @param tableNames []
	 * @param types [] @see BookingTable
	 * @param timeQueryFile
	 * @param outputFile
	 * @param numQueries
	 * @throws IOException
	 */
	ColFamTestWithQueryThesholds(
			String tableNames[], 
			int types[], 
			int numTables,  
			String timeQueryFile, 
			String outputPath, 
			int thresholds[], 
			int numThresholds,
			long numQueries) throws IOException{
		
		this.queryFileName = timeQueryFile;
		this.outputPath = outputPath;
		this.thresholds = thresholds;
		this.numQueries = numQueries;
		this.tableNames = tableNames;
		this.tableTypes = types;
		this.numTables = numTables;
		this.numThresholds = numThresholds;
		
	}
	
	
	public enum QueryType {scan_plus_preaggregation, scan_minus_preaggregation, scan_non_preaggregation
		,coproc_plus_preaggregation, coproc_minus_preaggregation, coproc_non_preaggregation};
	
	
	
	
	public void test(QueryType qtype) throws Throwable{
		
		Random random = new Random(100);
		
		BookingTable tables [] = new BookingTable[this.numTables];

		
		String outputFileName[] = new String [numThresholds];
		
		for(int i = 0; i < numThresholds; i ++){
			outputFileName[i] = this.outputPath + "t=" + thresholds[i] + "_" + qtype.toString();
		}
		
		System.out.println("queries File: " + this.queryFileName);
		System.out.println("output Files : ");
		for(int i = 0; i < numThresholds; i ++){
			System.out.println(outputFileName[i]);
		}
		System.out.println(qtype.toString() + " start Testing..");
		
		
		File file = new File(this.queryFileName);
		
		BufferedReader br = new BufferedReader ( new FileReader(file));
		
		BufferedWriter bw[] = new BufferedWriter[numThresholds];
		
		for(int i = 0; i < numThresholds; i ++){
			bw[i] = new BufferedWriter( new FileWriter (outputFileName[i]));
		}
		
		long startTime = 0;
		long endTime = 0;
		
		for(int i = 0; i < numTables; i ++){
			tables[i] = new BookingTable(tableNames[i], tableTypes[i]);
		}
		
		long resultCount = 0;
		long rowCount = 0;
		
		for(; rowCount < numQueries && br.ready(); ){
			String line = br.readLine();
			String items[] = line.split(",");
			String columns[] = new String [109];
			String startDate;
			String endDate;
			for(int m = 0; m < 109; m ++){
				columns[m] = items[m];				
			}
			startDate = transferDateFormat(items[109]);
			endDate = transferDateFormat(items[110]);
			int numProgIDs = items.length - 111;
			int progId = 0;
			double []result = null;
			
			int daysInterval = this.calculateTimeInterval(startDate, endDate);
			
			if(daysInterval < thresholds[0]){
				continue;
			}
			
			for(int m = 0; m < numProgIDs; m ++ ){
				Query q = new Query (columns, false);
			
				if(random.nextFloat()<0.8){
					progId = Integer.parseInt(items[111 + m]) % numActiveProgs;					
				}else{
					progId = Integer.parseInt(items[111 + m]) % numProgs;		 
				}
				
				String resultLine = new String();
				long responseTime [] = new long [numTables];
				
				switch(qtype){
				case coproc_plus_preaggregation:			
					for(int i = 0; i < numTables; i ++){
						startTime = System.nanoTime();
						result = tables[i].coprocPreAggregate(q, progId, startDate, endDate, 2, true);
						endTime = System.nanoTime();
						responseTime[i] = endTime - startTime;
						if(resultCount % 1000 == 0){
							System.out.println("Coproc_Plus-Preaggregation "+ resultCount + " table: " + tableNames[i] + " progId: " + progId + "[" + startDate + "," + endDate + "]" + " Response Time:" + (endTime - startTime)/1000/1000 + "ms" + " results: " + this.doubleArrayToString(result));
						}
					}
					
					break;
				case coproc_minus_preaggregation:
					for(int i = 0; i < numTables; i ++){
						startTime = System.nanoTime();
						result = tables[i].coprocPreAggregate(q, progId, startDate, endDate, 2, false);
						endTime = System.nanoTime();
						responseTime[i] = endTime - startTime;
						if(resultCount % 1000 == 0){
							System.out.println("Coproc_Minus-Preaggregation " + resultCount + " table: " + tableNames[i] + " progId: " + progId + "[" + startDate + "," + endDate + "]" + " Response Time:" + (endTime - startTime)/1000/1000 + "ms" + " results: " + this.doubleArrayToString(result));
						}
					}
					break;
				case coproc_non_preaggregation:
					for(int i = 0; i < numTables; i ++){
						startTime = System.nanoTime();
						result = tables[i].coprocAggregate(q, progId, startDate, endDate, 0);	
						endTime = System.nanoTime();
						responseTime[i] = endTime - startTime;
						if(resultCount % 1000 == 0){
							System.out.println("Coproc_Non-Preaggregation " + resultCount + " table: " + tableNames[i] + " progId: " + progId + "[" + startDate + "," + endDate + "]" + " Response Time:" + (endTime - startTime)/1000/1000 + "ms" + " results: " + this.doubleArrayToString(result));
						}
					}
					break;
				case scan_plus_preaggregation:
					for(int i = 0; i < numTables; i ++){
						startTime = System.nanoTime();
						result = tables[i].scanPreAggregate(q, progId, startDate, endDate, 2, true);
						endTime = System.nanoTime();
						responseTime[i] = endTime - startTime;
						if(resultCount % 1000 == 0){
							System.out.println("Scan_Plus-Preaggreagtion " + resultCount + " table: " + tableNames[i] + " progId: " + progId + "[" + startDate + "," + endDate + "]" + " Response Time:" + (endTime - startTime)/1000/1000 + "ms"+ " results: " + this.doubleArrayToString(result));
						}
					}
					break;
				case scan_minus_preaggregation:
					for(int i = 0; i < numTables; i ++){
						startTime = System.nanoTime();
						result = tables[i].scanPreAggregate(q, progId, startDate, endDate, 2, false);
						endTime = System.nanoTime();
						responseTime[i] = endTime - startTime;
						if(resultCount % 1000 == 0){
							System.out.println("Scan_Minus-Preaggregation " + resultCount + " table: " + tableNames[i] + " progId: " + progId + "[" + startDate + "," + endDate + "]" + " Response Time:" + (endTime - startTime)/1000/1000 + "ms"+ " results: " + this.doubleArrayToString(result));
						}
					}
					break;
				case scan_non_preaggregation:
					for(int i = 0; i < numTables; i ++){
						startTime = System.nanoTime();
						result = tables[i].scanAggregate(q, progId, startDate, endDate, 0);	
						endTime = System.nanoTime();
						responseTime[i] = endTime - startTime;
						if(resultCount % 1000 == 0){
							System.out.println("Scan_Non-Preagregation " + resultCount + " table: " + tableNames[i] + " progId: " + progId + "[" + startDate + "," + endDate + "]" + " Response Time:" + (endTime - startTime)/1000/1000 + "ms"+ " results: " + this.doubleArrayToString(result));
						}
					}
					break;
				}
				
				resultLine += resultCount + " " + daysInterval + " ";
				
				for(int i = 0; i < numTables; i ++){
					if(i != (numTables -1)){
						resultLine += responseTime[i] + " "	;					
					}else{
						resultLine += responseTime[i] + "\n";
					}
				}
				
				
				for(int i = 0; i < numThresholds; i++){
					if( daysInterval >= thresholds[i]){
//						System.out.println(resultCount + " " +i + " " + thresholds[i] + " " + resultLine + " [" + startDate + "," + endDate + "]");
						bw[i].write(resultLine);
						bw[i].flush();
					}
				}
						
				resultCount ++;
			}	
			rowCount ++;
		}
		
		for(int i = 0; i < numTables; i ++){

			bw[i].flush();
			bw[i].close();
			tables[i].close();
		}
		br.close();
	}
	
	
	private String transferDateFormat(String date) throws ParseException{
		return this.DATE_FORMAT_LONG.format(this.DATE_FORMAT_SHORT.parse(date));		
	}
	
	private String doubleArrayToString (double []result){
		String s = new String();
		s+="[";
		for (int i = 0; i < result.length; i ++){
			s += result[i] +"; ";
		}
		s += "]";
		return s;
	}
	
	private int calculateTimeInterval(String startDate, String endDate) throws ParseException{
		Date start = DATE_FORMAT_LONG.parse(startDate);
		Date end = DATE_FORMAT_LONG.parse(endDate);
		long diff = end.getTime() - start.getTime();
		int days = (int) ((diff+1) / (1000 * 60 * 60 * 24));
		return days;
	}
	
	public static int getRowCount (String filename) throws IOException{
		File file = new File(filename);
		BufferedReader br = new BufferedReader(new FileReader(file));
		int count = 0;
		while(br.ready()){
			String line = br.readLine();
			count ++;
		}
		
		br.close();
		return count;
	}
	
	

	public static void main (String [] args) throws Throwable{

		
		String tableNames[] = { "booking_10", "booking_11", "booking_12", "booking_13"};
		int types[] = {0, 1, 2, 3}; 
		int numTables = 4;
		
		String timeQueryFile = "QuerySetWithProgIdAndTimeRange/ProgIdTimeQueries";
		
		int numThresholds = 6;
		int thresholds[] = {6, 30, 60, 180, 365, 730};

		
		String outputPath = "ColFamsTestAggregation/";
		
		
		long numQueries = getRowCount(timeQueryFile);
		
		System.out.println("numQueries: " + numQueries);
		
		ColFamTestWithQueryThesholds colFamsTest = new ColFamTestWithQueryThesholds(
				tableNames, 
				types, 
				numTables,  
				timeQueryFile, 
				outputPath, 
				thresholds, 
				numThresholds,
				numQueries);
		
		colFamsTest.test(QueryType.coproc_non_preaggregation);
		colFamsTest.test(QueryType.coproc_plus_preaggregation);
		colFamsTest.test(QueryType.coproc_minus_preaggregation);
		
		colFamsTest.test(QueryType.scan_non_preaggregation);
		colFamsTest.test(QueryType.scan_plus_preaggregation);
		colFamsTest.test(QueryType.scan_minus_preaggregation);

	}
}