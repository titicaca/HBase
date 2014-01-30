package com.zanox.statistics.hbase.colfamsbuilding.coprocs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zanox.statistics.hbase.colfamsbuilding.importer.TypeRowKey;


public class ColumnValueCountEndPoint extends BaseEndpointCoprocessor implements ColumnValueCountProtocol{
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ColumnValueCountEndPoint.class);

	public double[] getValue(byte[] startRow, byte[] endRow, byte[][] colFam, byte[][] qualifier, int size) throws IOException {
		LOGGER.trace("start executing getValue method..");
		LOGGER.trace("start row: " + new TypeRowKey(startRow).toString() + "stop row: " + new TypeRowKey(endRow).toString());
		Scan scan = new Scan(startRow, endRow);
		scan.setLoadColumnFamiliesOnDemand(true);
		double [] result = new double [size]; 
		
		for(int i = 0; i < size; i ++){
			scan.addColumn(colFam[i], qualifier[i]);
		}
		
		RegionCoprocessorEnvironment environment = (RegionCoprocessorEnvironment) getEnvironment();
		InternalScanner scanner = environment.getRegion().getScanner(scan);
		long startTime = System.nanoTime();
		LOGGER.trace("Get Region:" + environment.getRegion().toString());
		try{
			List <KeyValue> curVals = new ArrayList<KeyValue>();
			boolean done = false;
			do{
				long start = System.nanoTime();
				curVals.clear();
				done = scanner.next(curVals);
				for(KeyValue kv : curVals){
					
					LOGGER.trace("scanning key value: " + new TypeRowKey(kv.getRow()).toString() + " family: " + Bytes.toString(kv.getFamily()) + " qualifier: " + Bytes.toInt(kv.getQualifier()));
					for(int i = 0; i < size; i++){
						if( isEqual(colFam[i] , kv.getFamily()) && isEqual(qualifier[i] , kv.getQualifier())){
							if(kv.getValue() == null){
								result[i] += 0;
							}else{
								result[i] += (double)Bytes.toFloat(kv.getValue());								
							}
							break;
						}
					}
				}
				LOGGER.trace("Iteration Scanning Time:" + (System.nanoTime() - start)/1000000 + "ms");
			}while(done);
		}finally{
			scanner.close();
		}		
		LOGGER.trace("Executing Time:" + (System.nanoTime() - startTime )/1000000 + "ms");

		return result; 
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