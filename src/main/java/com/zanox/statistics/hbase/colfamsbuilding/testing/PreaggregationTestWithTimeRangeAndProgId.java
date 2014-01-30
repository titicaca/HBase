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


public class PreaggregationTestWithTimeRangeAndProgId{
	
	long queryTime[][] = null;

//	final int MAX_PROGID = 10000;
//	final int MAX_PROGID = 500;
	
	static int numProgs = 10000;
	static int numActiveProgs = 500;
	Random random = new Random(100);
	BookingTable table;
	String tableName;
	int tableType;
	
	// query file extracted by extractTimeQuery() from class QueryLogExtractor
	final String queryFileName ;
	final String outputFileName;
	int numQueries;
	
	SimpleDateFormat DATE_FORMAT_SHORT = new SimpleDateFormat("yyyyMMdd");
	SimpleDateFormat DATE_FORMAT_LONG = new SimpleDateFormat("yyyy-MM-dd");
	
	/**
	 * 
	 * @param tableName
	 * @param type @see BookingTable
	 * @param timeQueryFile
	 * @param outputFile
	 * @param numQueries
	 * @throws IOException
	 */
	PreaggregationTestWithTimeRangeAndProgId(String tableName, int type, String timeQueryFile, String outputFile, int numQueries) throws IOException{
		
		this.queryFileName = timeQueryFile;
		this.outputFileName = outputFile;
		this.numQueries = numQueries;
		this.tableName = tableName;
		this.tableType = type;
		queryTime = new long[3][numQueries];
	}
	
	public enum QueryType {plus_preaggregation, minus_preaggregation, non_preaggregation};
	
	
	
	public void test(boolean isCoproc) throws Throwable{
		
		if(isCoproc){
			System.out.println("output file: " + this.outputFileName);
			System.out.println("start testing by coprocessor..");
			coprocRecord(QueryType.plus_preaggregation);
			coprocRecord(QueryType.minus_preaggregation);
			coprocRecord(QueryType.non_preaggregation);
			writeQueryTime(this.outputFileName);
		}else{
			System.out.println("output file: " + this.outputFileName);
			System.out.println("start testing by scanner..");
			scanRecord(QueryType.plus_preaggregation);
			scanRecord(QueryType.minus_preaggregation);
			scanRecord(QueryType.non_preaggregation);	
			writeQueryTime(this.outputFileName);
		}
	
	}
	
	private void coprocRecord(QueryType qType) throws Throwable{
		File file = new File(this.queryFileName);
		
		BufferedReader br = new BufferedReader ( new FileReader(file));
		long startTime = 0;
		long endTime = 0;
		table = new BookingTable(tableName, tableType);
		int resultCount = 0;

		for(; resultCount < numQueries && br.ready(); ){
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
			
			for(int m = 0; m < numProgIDs; m ++ ){
				Query q = new Query (columns, false);
			
				if(random.nextFloat()<0.8){
					progId = Integer.parseInt(items[111 + m]) % numActiveProgs;					
				}else{
					progId = Integer.parseInt(items[111 + m]) % numProgs;		 
				}

				switch(qType){
				case plus_preaggregation:					
					startTime = System.nanoTime();
					result = table.coprocPreAggregate(q, progId, startDate, endDate, 2, true);
					endTime = System.nanoTime();
					this.queryTime[0][resultCount] = endTime - startTime;
					if(resultCount % 100 == 0){
						System.out.println("Plus-Preaggregation "+ resultCount + " progId: " + progId + "[" + startDate + "," + endDate + "]" + " Response Time:" + (endTime - startTime)/1000/1000 + "ms" + " results: " + this.doubleArrayToString(result));
					}
					break;
				case minus_preaggregation:
					startTime = System.nanoTime();
					result = table.coprocPreAggregate(q, progId, startDate, endDate, 2, false);
					endTime = System.nanoTime();
					this.queryTime[1][resultCount] = endTime - startTime;
					if(resultCount % 100 == 0){
						System.out.println("Minus-Preaggregation " + resultCount + " progId: " + progId + "[" + startDate + "," + endDate + "]" + " Response Time:" + (endTime - startTime)/1000/1000 + "ms" + " results: " + this.doubleArrayToString(result));
					}
					break;
				case non_preaggregation:
					startTime = System.nanoTime();
					result = table.coprocAggregate(q, progId, startDate, endDate, 0);	
					endTime = System.nanoTime();
					this.queryTime[2][resultCount] = endTime - startTime;
					if(resultCount % 100 == 0){
						System.out.println("non-Preaggregation " + resultCount + " progId: " + progId + "[" + startDate + "," + endDate + "]" + " Response Time:" + (endTime - startTime)/1000/1000 + "ms" + " results: " + this.doubleArrayToString(result));
					}
					break;
				}		
				resultCount ++;
			}	
		}
		table.close();
		br.close();
	}
	
	private void scanRecord(QueryType qType) throws Throwable{
		File file = new File(this.queryFileName);

		BufferedReader br = new BufferedReader ( new FileReader(file));
		long startTime = 0;
		long endTime = 0;
		table = new BookingTable(tableName, tableType);
		int resultCount = 0;

		for(; resultCount < numQueries && br.ready();){
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

			for(int m = 0; m < numProgIDs; m ++ ){
				Query q = new Query (columns, false);
				
				if(random.nextFloat()<0.8){
					progId = Integer.parseInt(items[111 + m]) % numActiveProgs;					
				}else{
					progId = Integer.parseInt(items[111 + m]) % numProgs;		 
				}
				
				switch(qType){
				case plus_preaggregation:
					startTime = System.nanoTime();
					result = table.scanPreAggregate(q, progId, startDate, endDate, 2, true);
					endTime = System.nanoTime();
					this.queryTime[0][resultCount] = endTime - startTime;
					if(resultCount % 100 == 0){
						System.out.println("Plus-Preaggreagtion " + resultCount + " progId: " + progId + "[" + startDate + "," + endDate + "]" + " Response Time:" + (endTime - startTime)/1000/1000 + "ms"+ " results: " + this.doubleArrayToString(result));
					}
					break;
				case minus_preaggregation:
					startTime = System.nanoTime();
					result = table.scanPreAggregate(q, progId, startDate, endDate, 2, false);
					endTime = System.nanoTime();
					this.queryTime[1][resultCount] = endTime - startTime;
					if(resultCount % 100 == 0){
						System.out.println("Minus-Preaggregation " + resultCount + " progId: " + progId + "[" + startDate + "," + endDate + "]" + " Response Time:" + (endTime - startTime)/1000/1000 + "ms"+ " results: " + this.doubleArrayToString(result));
					}
					break;
				case non_preaggregation:
					startTime = System.nanoTime();
					result = table.scanAggregate(q, progId, startDate, endDate, 0);	
					endTime = System.nanoTime();
					this.queryTime[2][resultCount] = endTime - startTime;
					if(resultCount % 100 == 0){
						System.out.println("non-Preagregation " + resultCount + " progId: " + progId + "[" + startDate + "," + endDate + "]" + " Response Time:" + (endTime - startTime)/1000/1000 + "ms"+ " results: " + this.doubleArrayToString(result));
					}
					break;
				}	
				resultCount ++;
			}		
		}
		table.close();
		br.close();
	}
	
	void writeQueryTime(String resultFile) throws IOException{
		File file = new File(resultFile);
		BufferedWriter bw = new BufferedWriter(new FileWriter(file, false));
		for(int i = 0; i < numQueries; i++){
			String line = new String();
			for(int j = 0; j < queryTime.length; j++){
				line += queryTime[j][i];
				if(j != queryTime.length - 1){
					line += " ";
				}else{
					line += "\n";
				}
			}
			bw.write(line);
		}
		bw.close();
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
	
	public static void colFamsTest() throws Throwable{
		String timeQueryFile = "QuerySetWithProgIdAndTimeRange/ProgIdTimeQueries_t=366";
		int numQueries = getRowCount(timeQueryFile);
		String outputFile0 = "colFamsTest_Preaggregation/queryWithThreshold=366/avg_s0_scan";
		String outputFile1 = "colFamsTest_Preaggregation/queryWithThreshold=366/avg_s1_scan";
		String outputFile2 = "colFamsTest_Preaggregation/queryWithThreshold=366/avg_s2_scan";
		String outputFile3 = "colFamsTest_Preaggregation/queryWithThreshold=366/avg_s3_scan";
		
		PreaggregationTestWithTimeRangeAndProgId test0 = new PreaggregationTestWithTimeRangeAndProgId("booking_10", 0, timeQueryFile, outputFile0, numQueries);
		test0.test(false);
	
		PreaggregationTestWithTimeRangeAndProgId test1 = new PreaggregationTestWithTimeRangeAndProgId("booking_11", 1, timeQueryFile, outputFile1, numQueries);
		test1.test(false);
		
		PreaggregationTestWithTimeRangeAndProgId test2 = new PreaggregationTestWithTimeRangeAndProgId("booking_12", 2, timeQueryFile, outputFile2, numQueries);
		test2.test(false);
		
		PreaggregationTestWithTimeRangeAndProgId test3 = new PreaggregationTestWithTimeRangeAndProgId("booking_13", 3, timeQueryFile, outputFile3, numQueries);
		test3.test(false);	
	}
	
	public static void colFamsTest_coproc() throws Throwable{
		String timeQueryFile = "QuerySetWithProgIdAndTimeRange/ProgIdTimeQueries_t=366";
		int numQueries = getRowCount(timeQueryFile);
		String outputFile0 = "colFamsTest_Preaggregation/queryWithThreshold=366/avg_s0_coproc";
		String outputFile1 = "colFamsTest_Preaggregation/queryWithThreshold=366/avg_s1_coproc";
		String outputFile2 = "colFamsTest_Preaggregation/queryWithThreshold=366/avg_s2_coproc";
		String outputFile3 = "colFamsTest_Preaggregation/queryWithThreshold=366/avg_s3_coproc";
		
		PreaggregationTestWithTimeRangeAndProgId test0 = new PreaggregationTestWithTimeRangeAndProgId("booking_10", 0, timeQueryFile, outputFile0, numQueries);
		test0.test(true);
	
		PreaggregationTestWithTimeRangeAndProgId test1 = new PreaggregationTestWithTimeRangeAndProgId("booking_11", 1, timeQueryFile, outputFile1, numQueries);
		test1.test(true);
		
		PreaggregationTestWithTimeRangeAndProgId test2 = new PreaggregationTestWithTimeRangeAndProgId("booking_12", 2, timeQueryFile, outputFile2, numQueries);
		test2.test(true);
		
		PreaggregationTestWithTimeRangeAndProgId test3 = new PreaggregationTestWithTimeRangeAndProgId("booking_13", 3, timeQueryFile, outputFile3, numQueries);
		test3.test(true);		
	}

	public static void main (String [] args) throws Throwable{
//		String timeQueryFile = "ProgIdTimeQueries_threshold=64";
//		String outputFile = "PreaggregationResult/PreaggregationTestWithTimeRangeAndProgID_treshold=64_scan_sparse";
//		int numQueries = getRowCount(timeQueryFile);
////		int numQueries = 100000;
//		PreaggregationTestWithTimeRangeAndProgId test = new PreaggregationTestWithTimeRangeAndProgId("booking_11", 1, timeQueryFile, outputFile, numQueries);
//		boolean isCoproc = false;
//		test.test(isCoproc);
		
		colFamsTest();
		colFamsTest_coproc();
	}
}