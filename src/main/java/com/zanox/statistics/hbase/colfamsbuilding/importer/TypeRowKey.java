package com.zanox.statistics.hbase.colfamsbuilding.importer;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.gotometrics.orderly.IntWritableRowKey;
import com.gotometrics.orderly.RowKey;
import com.gotometrics.orderly.StructRowKey;
import com.gotometrics.orderly.TextRowKey;


public class TypeRowKey{

	private int type;
	private int progId;
	private String date;
	private final StructRowKey KEY_FORMAT = new StructRowKey(
			new RowKey[] { new IntWritableRowKey(), new IntWritableRowKey(), new TextRowKey() });
	private IntWritable TYPE_PROTOTYPE = new IntWritable();
	private IntWritable PROG_ID_PROTOTYPE = new IntWritable();
	private Text DATE_PROTOTYPE = new Text();
	
	
	/**
	 * Be careful with the format of the date string
	 * @param keytype 0--hour  1--day  2--month
	 * @param prog
	 * @param time
	 */
	public TypeRowKey (int keytype, int prog, String time){
		type = keytype;
		progId = prog;
		date = time;
	}
	
	
	public TypeRowKey (byte [] rowkey) throws IOException{
		Object[] dest = (Object[]) KEY_FORMAT.deserialize(rowkey);	
		date = Bytes.toString( ((Text)dest[2]).getBytes() );
		progId = ((IntWritable)dest[1]).get();
		type = ((IntWritable)dest[0]).get();

	}
	
	public byte[] getBytes() throws IOException{
		
		TYPE_PROTOTYPE.set(type);
		PROG_ID_PROTOTYPE.set(progId);
		DATE_PROTOTYPE.set(date);
		
		return KEY_FORMAT.serialize(new Object[]{TYPE_PROTOTYPE, PROG_ID_PROTOTYPE, DATE_PROTOTYPE});
	}
	
	public String getTime(){
		return this.date;
	}

	
	@Override
	public String toString() {
		return "RowKey [type=" +type + ", progId=" + progId + ", date=" + date +"]";
	}


	public int getProgId() {
		return this.progId;
	}
	
	public int getType(){
		return this.type;
	}
	
}