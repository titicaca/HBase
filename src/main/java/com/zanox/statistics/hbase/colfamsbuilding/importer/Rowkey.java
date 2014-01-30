package com.zanox.statistics.hbase.colfamsbuilding.importer;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.gotometrics.orderly.IntWritableRowKey;
import com.gotometrics.orderly.TextRowKey;
import com.gotometrics.orderly.RowKey;
import com.gotometrics.orderly.StructRowKey;

/**
 * A Rowkey class which store the rowkey info and transform methods
 * Rowkey Structure: progid(int) + date(String)
 * @author fangzhou.yang
 *
 */
public class Rowkey{
	private int progId;
	private String date;
	private final StructRowKey KEY_FORMAT = new StructRowKey(
			new RowKey[] { new IntWritableRowKey(), new TextRowKey() });
	private IntWritable PROG_ID_PROTOTYPE = new IntWritable();
	private Text DATE_PROTOTYPE = new Text();
	
	
	public Rowkey (int prog, String time){
		progId = prog;
		date = time;
	}
	
	
	public Rowkey (byte [] rowkey) throws IOException{
		Object[] dest = (Object[]) KEY_FORMAT.deserialize(rowkey);	
		date = Bytes.toString( ((Text)dest[1]).getBytes() );
		progId = ((IntWritable)dest[0]).get();
	}
	
	public byte[] getBytes() throws IOException{
		PROG_ID_PROTOTYPE.set(progId);
		DATE_PROTOTYPE.set(date);
		return KEY_FORMAT.serialize(new Object[]{PROG_ID_PROTOTYPE, DATE_PROTOTYPE});
	}
	
	public String getDate(){
		return this.date;
	}
	
	public int getProgId(){
		return this.progId;
	}
	

	
	@Override
	public String toString() {
		return "RowKey [progId=" + progId + ", date=" + date +"]";
	}
}