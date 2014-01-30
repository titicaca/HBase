package com.zanox.statistics.hbase.colfamsbuilding.importer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zanox.statistics.hbase.colfamsbuilding.algo.types.Attribute;
import com.zanox.statistics.hbase.colfamsbuilding.algo.types.ColFam;


/**
 * Transfer the data in the exsting htable (rowkey: progId-Date)
 * into a new htable and preaggregate the date (per day) to higher level (month)
 * @author fangzhou.yang
 *
 */
public class PreaggregateDataTransfer{
	private String tableName;
	
	private HTablePool pool;
	private HTableInterface hTable;
	
	private static Configuration conf = null;
	private static Integer bulkSize = 2097152;

	static SimpleDateFormat DATE_FORMAT= new SimpleDateFormat("yyyy-MM-dd");
	static SimpleDateFormat MONTH_FORMAT= new SimpleDateFormat("yyyy-MM");
	static SimpleDateFormat HOUR_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HH");

	private static final Logger LOGGER = LoggerFactory.getLogger(PreaggregateDataTransfer.class);

	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "t-cloudera-01.zanox.com");
	}
	
	ArrayList<ColFam> colfams = new ArrayList<ColFam> ();
	
	public void loadColFams(String structureFile) throws IOException{
		File file = new File(structureFile);
		BufferedReader br = new BufferedReader (new FileReader(file));
		int num = Integer.parseInt(br.readLine());
		int no = 0;
		while(br.ready()){
			no++;
			String line = br.readLine();
			String items[] = line.split(" ");
			ColFam c = new ColFam(items, ("cf"+no) );
			colfams.add(c);	
		}
		br.close();
	}
	
	public PreaggregateDataTransfer(String tableName) throws IOException{
		this.tableName = tableName;
		
		pool = new HTablePool(conf, 10);
		
		hTable = pool.getTable(this.tableName);
		hTable.setAutoFlush(false);
		hTable.setWriteBufferSize(bulkSize);
		
	}
	
	public void transferHourToDay(byte [] startKey, byte[] endKey) throws IOException, ParseException{
		//
		
		Scan scan = new Scan(startKey, endKey);
//		Scan scan = new Scan();
		
		ResultScanner rs = hTable.getScanner(scan);
		float dayAggregation[] = new float[109];
		boolean isCalculated[] = new boolean [109];

		TypeRowKey lastRow = null;
		System.out.println("transfering day pre-aggregation..");
		int count = 0;
		for(Result r : rs){
			count ++;
			byte[] rowkey = r.getRow();
			TypeRowKey currentRow = new TypeRowKey(rowkey);
			if(currentRow.getType() != 0){
				continue;
			}
			if(lastRow != null){
				if(!isInOneDay(lastRow, currentRow)){
					
					TypeRowKey dayRowKey = new TypeRowKey(1, lastRow.getProgId(), getDayDate(lastRow.getTime()));
					
					Put putDay = new Put(dayRowKey.getBytes());
					for(ColFam c: colfams){
						for(Attribute a : c.getAtts()){
							if(dayAggregation[a.getID()] != 0)
								putDay.add(Bytes.toBytes(c.getName()), Bytes.toBytes(a.getID()), Bytes.toBytes(dayAggregation[a.getID()]));
							
						}
					}
					hTable.put(putDay);
					
					dayAggregation = new float[109];

				}
			}
			
			isCalculated = new boolean [109];
			for(KeyValue kv : r.raw()){
								
				int columnNo = Bytes.toInt(kv.getQualifier());
				float value = Bytes.toFloat(kv.getValue());
				
				if(!isCalculated[columnNo]){
					dayAggregation[columnNo] += value;
					isCalculated[columnNo] = true;
				}				
			}
			lastRow = currentRow;
			
			if(count % 10000 == 0){
				System.out.println(count + "RowKey: " + lastRow.toString());
			}
			
		}
		
		//transfer the last group of day rowkeys
		TypeRowKey dayRowKey = new TypeRowKey(1, lastRow.getProgId(), getDayDate(lastRow.getTime()));
		

		Put putDay = new Put(dayRowKey.getBytes());
		for(ColFam c: colfams){
			for(Attribute a : c.getAtts()){
				if(dayAggregation[a.getID()] != 0)
					putDay.add(Bytes.toBytes(c.getName()), Bytes.toBytes(a.getID()), Bytes.toBytes(dayAggregation[a.getID()]));
					
			}
		}
		hTable.put(putDay);	
		
		
		hTable.flushCommits();
		
	}
	
	public void transferDayToMonth(byte [] startKey, byte[] endKey) throws IOException, ParseException{
		//byte [] startKey, byte[] endKey
		
		Scan scan = new Scan(startKey,  endKey);
		
		
		ResultScanner rs = hTable.getScanner(scan);
		float monthAggregation[] = new float[109];
		boolean isCalculated[] = new boolean [109];

		TypeRowKey lastRow = null;
		System.out.println("transfering month pre-aggregation..");
		int count = 0;
		for(Result r : rs){
			count ++;
			byte[] rowkey = r.getRow();
			TypeRowKey currentRow = new TypeRowKey(rowkey);
			if(currentRow.getType() != 1){
				continue;
			}
			if(lastRow != null){
				if(!isInOneMonth(lastRow, currentRow)){
					
					TypeRowKey monthRowKey = new TypeRowKey(2, lastRow.getProgId(), getMonthDate(lastRow.getTime()));
					
					Put putMonth = new Put(monthRowKey.getBytes());
					for(ColFam c: colfams){
						for(Attribute a : c.getAtts()){
							if(monthAggregation[a.getID()] != 0)
								putMonth.add(Bytes.toBytes(c.getName()), Bytes.toBytes(a.getID()), Bytes.toBytes(monthAggregation[a.getID()]));						
						}
					}
					hTable.put(putMonth);
					monthAggregation = new float[109];
				}
			}
			
			isCalculated = new boolean [109];
			for(KeyValue kv : r.raw()){
								
				int columnNo = Bytes.toInt(kv.getQualifier());
				float value = Bytes.toFloat(kv.getValue());
				
				if(!isCalculated[columnNo]){
					monthAggregation[columnNo] += value;
					isCalculated[columnNo] = true;
				}				
			}
			lastRow = currentRow;
			
			if(count % 10000 == 0){
				System.out.println(count + "RowKey: " + lastRow.toString());
			}
			
		}
		
		//transfer the last group of month rowkeys
		TypeRowKey monthRowKey = new TypeRowKey(2, lastRow.getProgId(), getMonthDate(lastRow.getTime()));
		
		Put putMonth = new Put(monthRowKey.getBytes());
		for(ColFam c: colfams){
			for(Attribute a : c.getAtts()){
				if(monthAggregation[a.getID()] != 0)
					putMonth.add(Bytes.toBytes(c.getName()), Bytes.toBytes(a.getID()), Bytes.toBytes(monthAggregation[a.getID()]));
				
			}
		}
		hTable.put(putMonth);		
	
		hTable.flushCommits();
		
	}
	
	public void close() throws IOException{
		this.hTable.close();
		this.pool.close();
	}
	
	
	private boolean isInOneDay(TypeRowKey r1, TypeRowKey r2) throws ParseException{
		Date d1 = HOUR_FORMAT.parse(r1.getTime());
		Date d2 = HOUR_FORMAT.parse(r2.getTime());
		if(d1.getYear() == d2.getYear() && d1.getMonth() == d2.getMonth() && d1.getDate() == d2.getDate() && r1.getProgId() == r2.getProgId()){
			return true;
		}else{
			return false;
		}
	}
	
	private boolean isInOneMonth(TypeRowKey r1, TypeRowKey r2) throws ParseException{
		Date d1 = DATE_FORMAT.parse(r1.getTime());
		Date d2 = DATE_FORMAT.parse(r2.getTime());
		if(d1.getYear() == d2.getYear() && d1.getMonth() == d2.getMonth() && r1.getProgId() == r2.getProgId()){
			return true;
		}else{
			return false;
		}
	}
	
	private String getDayDate(String date) throws ParseException{
		Date d = HOUR_FORMAT.parse(date);
		return DATE_FORMAT.format(d);
	}
	
	private String getMonthDate(String date) throws ParseException{
		Date d = DATE_FORMAT.parse(date);
		return MONTH_FORMAT.format(d);
	}
	
	public static void main(String [] args) throws IOException, ParseException{
		
		Date startDate = DATE_FORMAT.parse(HourDataImporter.startDateString);
		byte [] hourPrefix = new TypeRowKey(0, 0 , HOUR_FORMAT.format(startDate)).getBytes();
		byte [] dayPrefix = new TypeRowKey(1, 0 , DATE_FORMAT.format(startDate)).getBytes();
		byte [] monthPrefix = new TypeRowKey(2, 0 , MONTH_FORMAT.format(startDate)).getBytes();

		
		
		PreaggregateDataTransfer t0 = new PreaggregateDataTransfer("report00");
		t0.loadColFams("results\\structures\\s00");
		t0.transferHourToDay(hourPrefix, dayPrefix);
		t0.transferDayToMonth(dayPrefix, monthPrefix);
		t0.close();
		
		PreaggregateDataTransfer t1 = new PreaggregateDataTransfer("report01");
		t1.loadColFams("results\\structures\\s01");
		t1.transferHourToDay(hourPrefix, dayPrefix);
		t1.transferDayToMonth(dayPrefix, monthPrefix);
		t1.close();
		
		PreaggregateDataTransfer t2 = new PreaggregateDataTransfer("report02");
		t2.loadColFams("results\\structures\\s02");
		t2.transferHourToDay(hourPrefix, dayPrefix);
		t2.transferDayToMonth(dayPrefix, monthPrefix);
		t2.close();
		
		PreaggregateDataTransfer t3 = new PreaggregateDataTransfer("report03");
		t3.loadColFams("results\\structures\\s03");
		t3.transferHourToDay(hourPrefix, dayPrefix);
		t3.transferDayToMonth(dayPrefix, monthPrefix);
		t3.close();	
		
	}
}