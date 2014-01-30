package com.zanox.statistics.hbase.colfamsbuilding.coprocs;

import java.io.IOException;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface ColumnValueCountProtocol extends CoprocessorProtocol{
	
	double[] getValue(byte[] startRow, byte[] emdRow, byte[][] columnFamily, byte[][] qualifier, int size) throws IOException ;
	
}