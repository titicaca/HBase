package com.zanox.statistics.hbase.colfamsbuilding.testing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;

import com.zanox.statistics.hbase.colfamsbuilding.coprocs.ColumnValueCountProtocol;
import com.zanox.statistics.hbase.colfamsbuilding.importer.Constants;
import com.zanox.statistics.hbase.colfamsbuilding.importer.Rowkey;


public class CoprocVsScan{
	private static Configuration conf = null;
	static {
	     conf = HBaseConfiguration.create();
	     conf.set("hbase.zookeeper.quorum", "t-cloudera-01.zanox.com");
	}
	
	private static HTablePool pool = null;
		
	private static HTableInterface htable = null;
	
	String queryFileName = "coprocVSscan";
	
	int numQueries;
	
	double [][]queryTime = null;
	
	String startDate = "2004-01-01";
    String endDate = "2014-01-01";
    
    public void setTimeRange(String start, String end){
    	this.startDate = start;
    	this.endDate = end;
    }
	
    CoprocVsScan(int queries){
		pool = new HTablePool(conf, 10);
		htable = pool.getTable("booking_00");
		numQueries = queries;
		queryTime = new double [2][numQueries];
    }
	
	public void test(Random random) throws Throwable{
		
		long startTime = 0;
		long endTime = 0;
		long responseTime = 0;
		
		for(int m = 0; m < numQueries; m ++){
			System.out.println("Query - " + m);
			
			int progId = random.nextInt(Constants.MAX_NUM_PROGS);
			
			final int size = 109;
			final byte[][] columnFamily = new byte[size][Bytes.toBytes("cf1").length];
			final byte[] startRow = new Rowkey(progId,startDate).getBytes();
			final byte[] endRow = new Rowkey(progId,endDate).getBytes();
			final byte[][] qualifier = new byte[size][Bytes.toBytes((int)0).length];
			
			for(int i = 0; i < size; i ++){
				columnFamily[i] = Bytes.toBytes("cf1");
				qualifier[i] = Bytes.toBytes(i);
			}
			
			startTime = System.nanoTime();
			double result1[] = queryWithCoproc(startRow, endRow, columnFamily, qualifier, size);
			endTime = System.nanoTime();
			responseTime = (endTime - startTime)/1000/1000;
			queryTime[0][m] = responseTime;
			
			startTime = System.nanoTime();
			double result2[] = queryWithScan(startRow, endRow, columnFamily, qualifier, size);
			endTime = System.nanoTime();
			responseTime = (endTime - startTime)/1000/1000;
			queryTime[1][m] = responseTime;					
		}		
	}
	
	public double[] queryWithScan(byte[] startRow, byte[] endRow,
			byte[][] columnFamily, byte[][] qualifier, int size) throws IOException {
		
    	double result[] = new double[size]; 
    	 Scan scan = new Scan();
    	 scan.setStartRow(startRow);
    	 scan.setStopRow(endRow);
    	 
 		for(int i = 0; i < size; i ++){
			scan.addColumn(columnFamily[i], qualifier[i]);
		}
 		
 		ResultScanner rs = htable.getScanner(scan);
 		int count = 0;
 		for(Result r : rs){
 			count++;
 			for(KeyValue kv : r.raw()){
 				for(int i = 0; i < size; i++){
					if( isEqual(columnFamily[i] , kv.getFamily()) && isEqual(qualifier[i] , kv.getQualifier())){
						if(kv.getValue() == null){
							result[i] += 0;
						}else{
							result[i] += (double)Bytes.toFloat(kv.getValue());								
						}
						break;
					}
				}
 			}
 			Rowkey rowkey = new Rowkey(r.getRow());
			System.out.println(count + "RowKey: " + rowkey.toString());

 		}
 		
 		return result;

	}

	private double[] queryWithCoproc(final byte[] startRow, final byte[] endRow,
			final byte[][] columnFamily, final byte[][] qualifier, final int size) throws IOException, Throwable {

		    	
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
    	
    	double result[] = new double[size]; 
    	
    	for (Entry<byte[], double[]> entry: results.entrySet()){
    		
    		for(int i = 0; i < size; i++){
    			result[i] += entry.getValue()[i];
    		}	    	
    	}
    	
    	return result;
	}

	void writeQueryTime(String resultFile) throws IOException{
		File file = new File(resultFile);
		BufferedWriter bw = new BufferedWriter(new FileWriter(file, false));
		for(int i = 0; i < numQueries; i++){
			String line = new String();
			line = queryTime[0][i] + " " + queryTime[1][i] + "\n";
				
			bw.write(line);
		}
		bw.close();
	}
	
	boolean isEqual(byte[] b1, byte[] b2){
		
		if(b1.length != b2.length){
			return false;
		}
		else{
			for(int i = 0; i < b1.length; i ++){
				if(b1[i] != b2[i]){
					return false;
				}
			}
			return true;
		}		
	}	
	
	public static void main(String [] args) throws Throwable{
		Random random= new Random(100);
		CoprocVsScan test = new CoprocVsScan(100);
		test.setTimeRange("2013-01-01", "2014-01-01");
		test.test(random);
		test.writeQueryTime("CoprocsTest/coprocVsScan_5year_100");
				
//		int progId = 1;
//		final int size = 109;
//		final byte[][] columnFamily = new byte[size][Bytes.toBytes("cf1").length];
//		final byte[] startRow = new Rowkey(progId,test.startDate).getBytes();
//		final byte[] endRow = new Rowkey(progId,test.endDate).getBytes();
//		final byte[][] qualifier = new byte[size][Bytes.toBytes((int)0).length];
//		
//		for(int i = 0; i < size; i ++){
//			columnFamily[i] = Bytes.toBytes("cf1");
//			qualifier[i] = Bytes.toBytes(i);
//		}
//		
//		double result[] = test.queryWithScan(startRow, endRow, columnFamily, qualifier, size);
//		
//		for(int i = 0; i < 109; i++){
//			System.out.println(result[i]);
//		}
	}
}