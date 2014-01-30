package com.zanox.statistics.hbase.colfamsbuilding.coprocs;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;

import com.zanox.statistics.hbase.colfamsbuilding.importer.Rowkey;

public class ColumnValueCountClient{
	public static void main(String[] args) throws IOException{
		Configuration conf = HBaseConfiguration.create();
		
		conf.set("hbase.zookeeper.quorum", "t-cloudera-01.zanox.com");
	    conf.set("hbase.zookeeper.property.clientPort", "2181");
	    conf.set("hbase.coprocessor.region.classes", "coprocessor.ColumnValueCountEndPoint");
	    

	    int progId = 10;
	    String startDate = "2004-01-01";
	    String endDate = "2004-02-01";
	    
	    HTable table = new HTable(conf, "booking_11");
	    

	    
	    try{
	    	final int size = 109;
			final byte[][] columnFamily = new byte[size][Bytes.toBytes("cf1").length];
			final byte[] startRow = new Rowkey(progId,startDate).getBytes();
			final byte[] endRow = new Rowkey(progId,endDate).getBytes();
			final byte[][] qualifier = new byte[size][Bytes.toBytes((int)0).length];
			
			for(int i = 0; i < size; i ++){
				columnFamily[i] = Bytes.toBytes("cf1");
				qualifier[i] = Bytes.toBytes(i);
			}
			
			long startTime = System.nanoTime();
	    	
	    	Map<byte[], double[]> results = table.coprocessorExec(ColumnValueCountProtocol.class,
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
	    	
	    	for(int i = 0; i < size; i++){
	    		System.out.println("column " + i +" : "+ result[i]);
	    	}
	    	
	    	long endTime = System.nanoTime();
			long responseTime = (endTime - startTime);
			
			System.out.println("coproc response time: " + responseTime/1000/1000 + "ms");
			
			System.out.println("-------------------------------------------------");

			
			startTime = System.nanoTime();
			
			String cf = "cf1";
			double scanResult[] = new double [size];
		    Scan scan = new Scan();
		    scan.setStartRow(startRow);
		    scan.setStopRow(endRow);
		    ResultScanner rs = table.getScanner(scan);
		    for(Result r: rs){   
                for(KeyValue kv : r.raw()){ 
                    scanResult[Bytes.toInt(kv.getQualifier())] += Bytes.toFloat(kv.getValue());
                }   
            } 
		    
		    for(int i = 0; i < size; i++){
	    		System.out.println("column " + i +" : "+ scanResult[i]);
	    	}

		    endTime = System.nanoTime();
			responseTime = (endTime - startTime);
			
			System.out.println("scan response time: " + responseTime/1000/1000 + "ms");
	    	
	    }catch(Throwable throwable){
	    		throwable.printStackTrace();
	    }
	    
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
}
