package com.zanox.statistics.hbase.colfamsbuilding.testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zanox.statistics.hbase.colfamsbuilding.algo.types.ColFam;
import com.zanox.statistics.hbase.colfamsbuilding.algo.types.Query;
import com.zanox.statistics.hbase.colfamsbuilding.coprocs.ColumnValueCountProtocol;
import com.zanox.statistics.hbase.colfamsbuilding.importer.HourDataImporter;
import com.zanox.statistics.hbase.colfamsbuilding.importer.Rowkey;
import com.zanox.statistics.hbase.colfamsbuilding.importer.TypeRowKey;

/**
 * Class of Operations for Booking Table
 * @author fangzhou.yang
 *
 */
public class BookingTable{
	private static final Logger LOGGER = LoggerFactory.getLogger(BookingTable.class);

	private final String STRUCTURE_FILE00 = "results//structures//s00";
	private final String STRUCTURE_FILE01 = "results//structures//s01";
	private final String STRUCTURE_FILE02 = "results//structures//s02";
	private final String STRUCTURE_FILE03 = "results//structures//s03";

	static SimpleDateFormat HOUR_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HH");
	static SimpleDateFormat DATE_FORMAT= new SimpleDateFormat("yyyy-MM-dd");
	static SimpleDateFormat MONTH_FORMAT= new SimpleDateFormat("yyyy-MM");
	
	private static Configuration conf = null;
	static{
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "t-cloudera-01.zanox.com");
	}
	
	private HTablePool pool = null;
	private HTableInterface htable = null;
	
	
	private String tableName;
	ArrayList<ColFam> colfams = new ArrayList<ColFam>();

	
	/**
	 * Construct function
	 * @param tablename 
	 * @param type type is the number of the column family structure, from 0 to 3
	 * @throws IOException
	 */
	public BookingTable(String tablename, int type) throws IOException{
		tableName = tablename;
		switch (type){
			case 0:
				loadColFams(STRUCTURE_FILE00);
				break;
			case 1:
				loadColFams(STRUCTURE_FILE01);
				break;
			case 2:
				loadColFams(STRUCTURE_FILE02);
				break;
			case 3:
				loadColFams(STRUCTURE_FILE03);
				break;
			default:
				colfams = null;
		}
		
		pool = new HTablePool(conf, 10);
		htable = pool.getTable(tableName);
	}
	
	
	public void close() throws IOException{
		htable.close();
		pool.close();
	}
	
	/**
	 * Initialize column families
	 * load column family structure from the local file
	 * @param structureFile
	 * @throws IOException
	 */
	private void loadColFams(String structureFile) throws IOException{
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
	
	/**
	 * Scan for specific columns for a given progId and time range and return an aggregated result
	 * @param q: query q contains the specific columns to be queried.
	 * @param progId 
	 * @param startDate
	 * @param endDate
	 * @param type: table type: 0--hour table, 1 --day table, 2--month table
	 * @return aggregated result, the column-sequence is the same with the columns of the query
	 * @throws IOException
	 */
	public double[] scanAggregate(Query q, int progId, String startDate, String endDate, int type) throws IOException{
		double [] result = new double[q.getAttNum()];
		
		final int size = q.getAttNum();
		final byte[][] columnFamily = new byte[size][Bytes.toBytes("cf1").length];
		final byte[] startRow;
		final byte[] endRow;
		
		startRow = new TypeRowKey(type, progId, startDate).getBytes();  
		endRow = new TypeRowKey(type, progId, endDate).getBytes();			
		
		final byte[][] qualifier = new byte[size][Bytes.toBytes((int)0).length];
		
		setQueryParameters(columnFamily, qualifier, q);
		
		result = queryWithScan(startRow, endRow, columnFamily, qualifier, size);
		
		return result;
	
	}
	
	/**
	 * Aggregation for specific columns for a given progId and time range via Coprocessor 
	 * @param q: query q contains the specific columns to be queried.
	 * @param progId 
	 * @param startDate
	 * @param endDate
	 * @param type: table type: 0--hour table, 1 --day table, 2--month table
	 * @return aggregated result, the column-sequence is the same with the columns of the query
	 * @throws Throwable
	 */
	public double[] coprocAggregate(Query q, int progId, String startDate, String endDate, int type) throws Throwable{
		double [] result = new double[q.getAttNum()];
		
		final int size = q.getAttNum();
		final byte[][] columnFamily = new byte[size][Bytes.toBytes("cf1").length];

		
		final byte[] startRow = new TypeRowKey(type, progId, startDate).getBytes();
		final byte[] endRow = new TypeRowKey(type, progId, endDate).getBytes();
		final byte[][] qualifier = new byte[size][Bytes.toBytes((int)0).length];
		
		setQueryParameters(columnFamily, qualifier, q);
		
//		printColumnFamilies(columnFamily, size);

		result = queryWithCoproc(startRow, endRow, columnFamily, qualifier, size);
		
		return result;
	
	}
	
	private void printColumnFamilies(byte [][] columnFamily, int size){
		System.out.print("columnFamilies: [");
		for(int i = 0; i < size; i ++){
			System.out.print(Bytes.toString(columnFamily[i]) + ",");
		}
		System.out.println("]");
	}
	
	public TimeRange buildTimeRange(int timeType, Date d1, Date d2){
		return new TimeRange(timeType, d1, d2);
	}
	
	/**
	 * 
	 * @author fangzhou.yang
	 *
	 */
	public class TimeRange{
		Date startDate;
		Date endDate;
		int type; // 0--hour 1--day 2--month
		boolean additive = true;
		
		TimeRange(int timeType, Date d1, Date d2){
			type = timeType;
			startDate = d1;
			endDate = d2;
		}
		
		public TimeRange(int timeType, Date d1, Date d2, boolean isAdditive){
			type = timeType;
			startDate = d1;
			endDate = d2;
			additive = isAdditive;
		}
		
		boolean isAdditive(){
			return additive;
		}
		
		String getStartDate(){
			if(type == 0){
				return HOUR_FORMAT.format(startDate);				
			}
			if(type == 1){
				return DATE_FORMAT.format(startDate);	
			}
			if(type == 2){
				return MONTH_FORMAT.format(startDate);
			}
			else{
				return null;
			}
		}
		String getEndDate(){
			if(type == 0){
				return HOUR_FORMAT.format(endDate);				
			}
			if(type == 1){
				return DATE_FORMAT.format(endDate);	
			}
			if(type == 2){
				return MONTH_FORMAT.format(endDate);
			}
			else{
				return null;
			}
		}
		
		@Override
		public String toString(){
			String output = new String();
			if(additive){
				output += "+";
			}else{
				output += "-";
			}
			output += "[" + getStartDate() + "," + getEndDate() + "]";
			return output;
			
		}
	}
	
	private ArrayList<TimeRange> decomposeTimeRanges(int level, TimeRange queryTimeRange, boolean isAdditiveLogic) throws ParseException{
		ArrayList<TimeRange>  timeRanges = new ArrayList<TimeRange> ();
		
		if(level == 0){
			timeRanges.add(queryTimeRange);
		}
		
		if( level >= 1 ){
			ArrayList<TimeRange> dayTimeRanges;
			if(isAdditiveLogic)
				dayTimeRanges = decomposeHourToDayTimeRange(queryTimeRange);
			else
				dayTimeRanges = decomposeHourToDayTimeRangeWithMinusLogik(queryTimeRange);
			
			timeRanges = dayTimeRanges;
		}
		
		if(level >= 2){
			ArrayList<TimeRange> monthTimeRanges;
			for(int i = 0; i < timeRanges.size(); i ++){
				TimeRange tr = timeRanges.get(i);
				if(tr.type == 1){
					timeRanges.remove(i);
					if(isAdditiveLogic)
						monthTimeRanges = decomposeDayToMonthTimeRange(tr);
					else
						monthTimeRanges = decomposeDayToMonthTimeRangeWithMinusLogik(tr);
					
					timeRanges.addAll(monthTimeRanges);
					break;
				}
			}
		}
		
		this.printTimeRanges(timeRanges);
		return timeRanges;
	}
	
//	/**
//	 * Decompose the given time range
//	 * Using day level preaggregated data to do scanAggregate.
//	 * @param q: query q contains the specific columns to be queried.
//	 * @param progId 
//	 * @param startDate
//	 * @param endDate
//	 * @param isAdditiveLogic
//	 * @return aggregated result, the column-sequence is the same with the columns of the query
//	 * @throws ParseException
//	 * @throws IOException
//	 */
//	public double[] scanDayPreAggregate(Query q, int progId, String startDate, String endDate, boolean isAdditiveLogic) throws ParseException, IOException{
//		ArrayList<TimeRange>  timeRanges = null;
//		
//		TimeRange queryTimeRange = new TimeRange (0, HOUR_FORMAT.parse(startDate), HOUR_FORMAT.parse(endDate));
//		
//		if(isAdditiveLogic){
//			timeRanges = decomposeHourToDayTimeRange(queryTimeRange);	
//		}else{
//			timeRanges = decomposeHourToDayTimeRangeWithMinusLogik(queryTimeRange);		
//		}
//		
//		double [] result = new double[q.getAttNum()];
//		for(int i = 0; i < timeRanges.size(); i ++){
//			TimeRange tr = timeRanges.get(i);
//			double [] tmp = scanAggregate(q, progId, tr.getStartDate(), tr.getEndDate(),tr.type );
//			for(int m = 0; m <result.length; m ++){
//				result[m] += tmp[m];
//			}
//		}
//		return result;		
//	}
	
	/**
	 * Decompose the given time range
	 * Using day level preaggregated data to do scanAggregate.
	 * @param q
	 * @param progId
	 * @param startDate
	 * @param endDate
	 * @param level 0--hour 1--day 2--month
	 * @param isAdditiveLogic
	 * @return aggregated result, the column-sequence is the same with the columns of the query
	 * @throws ParseException
	 * @throws IOException
	 */
	public double[] scanPreAggregate(Query q, int progId, String startDate, String endDate, int level, boolean isAdditiveLogic) throws ParseException, IOException{
		
		TimeRange queryTimeRange = new TimeRange (0, HOUR_FORMAT.parse(startDate), HOUR_FORMAT.parse(endDate));
		
		
		ArrayList<TimeRange>  timeRanges = this.decomposeTimeRanges(level, queryTimeRange, isAdditiveLogic);

		
		double [] result = new double[q.getAttNum()];
		for(int i = 0; i < timeRanges.size(); i ++){
			TimeRange tr = timeRanges.get(i);
			System.out.println("StartDate: " + tr.getStartDate() + " EndDate: " + tr.getEndDate());
			double [] tmp = scanAggregate(q, progId, tr.getStartDate(), tr.getEndDate(),tr.type );
			for(int m = 0; m <result.length; m ++){
				result[m] += tmp[m];
			}
		}
		return result;		
	}
	
	
	
	/**
	 * Decompose the given time range
	 * Using preaggregated data to do coprocAggregate.
	 * @param q: query q contains the specific columns to be queried.
	 * @param progId 
	 * @param startDate
	 * @param endDate
	 * @param level 0--hour 1--day 2--month
	 * @param isAdditiveLogic
	 * @return aggregated result, the column-sequence is the same with the columns of the query
	 * @throws Throwable
	 */
	public double[] coprocPreAggregate(Query q, int progId, String startDate, String endDate, int level, boolean isAdditiveLogic) throws Throwable{

		TimeRange queryTimeRange = new TimeRange (0, HOUR_FORMAT.parse(startDate), HOUR_FORMAT.parse(endDate));

		ArrayList<TimeRange>  timeRanges = this.decomposeTimeRanges(level, queryTimeRange, isAdditiveLogic);

		double [] result = new double[q.getAttNum()];
		for(int i = 0; i < timeRanges.size(); i ++){
			TimeRange tr = timeRanges.get(i);
			double [] tmp = coprocAggregate(q, progId, tr.getStartDate(), tr.getEndDate(),tr.type );
			for(int m = 0; m <result.length; m ++){
				if(tr.additive)
					result[m] += tmp[m];
				else
					result[m] -= tmp[m];
			}
		}
		return result;		
	}
	
	
	/**
	 * decompose hour level time range into day level and month level
	 * @param tr
	 * @return
	 * @throws ParseException
	 */
	private ArrayList<TimeRange> decomposeHourToDayTimeRange(TimeRange tr) throws ParseException{
		ArrayList<TimeRange>  timeRanges = new ArrayList<TimeRange>();
		
		if(tr.type != 0){
			return null;
		}
		
		Date startdate = HOUR_FORMAT.parse(tr.getStartDate());
		Date enddate = HOUR_FORMAT.parse(tr.getEndDate());
		
		Calendar startDate = Calendar.getInstance();
		startDate.setTime(startdate);
		Calendar endDate = Calendar.getInstance();
		endDate.setTime(enddate);
		Calendar tmpDate = Calendar.getInstance();
		tmpDate.setTime(startdate);
		
		boolean ending = false;
		
		if(startDate.get(Calendar.HOUR_OF_DAY) != 0 && !ending){
			Date startHour = startDate.getTime();
			Date endHour;
			//in the same day
			if(startDate.get(Calendar.YEAR)==endDate.get(Calendar.YEAR) 
					&& startDate.get(Calendar.MONTH)==endDate.get(Calendar.MONTH)
					&& startDate.get(Calendar.DAY_OF_MONTH) == endDate.get(Calendar.DAY_OF_MONTH)){
				endHour = endDate.getTime();
				ending = true;
			}else{
				startDate.set(Calendar.HOUR_OF_DAY,0);
				startDate.add(Calendar.DAY_OF_MONTH,1);				
				endHour = startDate.getTime();	
			}
			timeRanges.add(new TimeRange(0, startHour, endHour));
		}
		
		if(startDate.compareTo(endDate)==0){
			ending = true;
		}
		if(endDate.get(Calendar.HOUR_OF_DAY) != 0 && !ending){
			Date startHour;
			Date endHour = endDate.getTime();
			endDate.set(Calendar.HOUR_OF_DAY, 0);
			startHour = endDate.getTime();
			timeRanges.add(new TimeRange(0, startHour, endHour));	
		}
		if(startDate.compareTo(endDate)==0){
			ending = true;
		}
		if(!ending){
			Date startDay = startDate.getTime();
			Date endDay = endDate.getTime();
			timeRanges.add(new TimeRange(1, startDay, endDay));	
		}	
		
//		this.printTimeRanges(timeRanges);
		
		return timeRanges;	
	}
	
	private void printTimeRanges(ArrayList<TimeRange>  timeRanges){
		for(int i = 0; i < timeRanges.size(); i ++){
			System.out.println(timeRanges.get(i).toString());
		}
	}
	
	/**
	 * decompose day level time range into day level and month level 
	 * For example: [2011-02-08, 2011-07-02) = [2011-02-08, 2011-03-01) + [2011-03,2011-07) + [2011-07-01,2011-07-02)
	 * @param start
	 * @param end
	 * @return
	 * @throws ParseException
	 */
	private ArrayList<TimeRange> decomposeDayToMonthTimeRange(TimeRange tr) throws ParseException{
		
		ArrayList<TimeRange>  timeRanges = new ArrayList<TimeRange>();
		
		if(tr.type != 1){
			return null;
		}
		
		Date startdate = DATE_FORMAT.parse(tr.getStartDate());
		Date enddate = DATE_FORMAT.parse(tr.getEndDate());
		
		Calendar startDate = Calendar.getInstance();
		startDate.setTime(startdate);
		Calendar endDate = Calendar.getInstance();
		endDate.setTime(enddate);
		Calendar tmpDate = Calendar.getInstance();
		tmpDate.setTime(startdate);
		
		boolean ending = false;

		if(startDate.get(Calendar.DATE) != 1 && !ending){
			Date startDay = startDate.getTime();
			Date endDay;
			// in the same month
			if(startDate.get(Calendar.YEAR)==endDate.get(Calendar.YEAR) && startDate.get(Calendar.MONTH)==endDate.get(Calendar.MONTH)){
				endDay = endDate.getTime();
				ending = true;
			}else {
				startDate.set(Calendar.DATE,1);
				startDate.add(Calendar.MONTH,1);				
				endDay = startDate.getTime();	
			}
//			// in the next month
//			if(startDate.get(Calendar.YEAR)==endDate.get(Calendar.YEAR) && startDate.get(Calendar.MONTH)==endDate.get(Calendar.MONTH)){
//				endDay = endDate.getTime();
//				ending = true;
//			}
			timeRanges.add(new TimeRange(1,startDay, endDay));			
		}
		if(startDate.compareTo(endDate)==0){
			ending = true;
		}
		if(endDate.get(Calendar.DATE) != 1 && !ending){
			Date startDay;
			Date endDay = endDate.getTime();
			endDate.set(Calendar.DATE, 1);
			startDay = endDate.getTime();
			timeRanges.add(new TimeRange(1,startDay, endDay));	
		}
		if(startDate.compareTo(endDate)==0){
			ending = true;
		}
		if(!ending){
			Date startMonth = startDate.getTime();
			Date endMonth = endDate.getTime();
			timeRanges.add(new TimeRange(2,startMonth, endMonth));	
		}	
		return timeRanges;	
	}
	
	/**
	 * decompose hour level time range into day level and month level, but with a plus and minus logic
	 * @param tr
	 * @return
	 * @throws ParseException
	 */
	private ArrayList<TimeRange> decomposeHourToDayTimeRangeWithMinusLogik(TimeRange tr) throws ParseException{
		ArrayList<TimeRange>  timeRanges = new ArrayList<TimeRange>();
		
		if(tr.type != 0){
			return null;
		}
		
		Date startdate = HOUR_FORMAT.parse(tr.getStartDate());
		Date enddate = HOUR_FORMAT.parse(tr.getEndDate());
		
		Calendar startDate = Calendar.getInstance();
		startDate.setTime(startdate);
		Calendar endDate = Calendar.getInstance();
		endDate.setTime(enddate);
		Calendar tmpDate = Calendar.getInstance();
		tmpDate.setTime(startdate);
		
		boolean ending = false;
		boolean additive = true;

		
		if(startDate.get(Calendar.HOUR_OF_DAY) != 0 && !ending){
			Date startHour = startDate.getTime();
			Date endHour;		
			
			// in the same day
			if(startDate.get(Calendar.YEAR)==endDate.get(Calendar.YEAR) 
					&& startDate.get(Calendar.MONTH)==endDate.get(Calendar.MONTH)
					&& startDate.get(Calendar.DAY_OF_MONTH) == endDate.get(Calendar.DAY_OF_MONTH)){
				endHour = endDate.getTime();
				ending = true;
				additive = true;

			}else {
				// hh.dd.mm.yy -- 00.dd+1.mm.yy
				if(startDate.get(Calendar.HOUR_OF_DAY) > 8){
					startDate.set(Calendar.HOUR_OF_DAY,0);
					startDate.add(Calendar.DAY_OF_MONTH, 1);
					endHour = startDate.getTime();
					additive = true;
				}
				// -(00.dd.mm.yy -- hh.dd.mm.yy)
				else {
					endHour = startHour;
					startDate.set(Calendar.HOUR_OF_DAY,0);
					startHour = startDate.getTime();
					additive = false;
				}	
			}
				
			timeRanges.add(new TimeRange(0, startHour, endHour, additive));
			
		}
		
		if(startDate.compareTo(endDate)==0){
			ending = true;
		}
		
		if(endDate.get(Calendar.HOUR_OF_DAY) != 0 && !ending){
			Date startHour;
			Date endHour = endDate.getTime();
			
			//00.dd.mm.yy -- hh.dd.mm.yy
			if(endDate.get(Calendar.HOUR_OF_DAY) < 16){
				endHour = endDate.getTime();
				endDate.set(Calendar.HOUR_OF_DAY, 0);
				startHour = endDate.getTime();
				additive = true;
			}
			else{ //-(hh.dd.mm.yy -- 00.dd.mm.yy)
				startHour = endDate.getTime();
				endDate.add(Calendar.DAY_OF_MONTH, 1);
				endDate.set(Calendar.HOUR_OF_DAY, 0);
				endHour = endDate.getTime();
				additive = false;
			}
			

			timeRanges.add(new TimeRange(0, startHour, endHour, additive));	
		}
		if(startDate.compareTo(endDate)==0){
			ending = true;
		}
		if(!ending){
			Date startMonth = startDate.getTime();
			Date endMonth = endDate.getTime();
			timeRanges.add(new TimeRange(2, startMonth, endMonth));	
		}	
		return timeRanges;		
	}
	
	/**
	 * decompose the time range into day level and month level, but with a plus and minus logic
	 * for example: [2011-02-08, 2011-07-02) = -[2011-02-01, 2011-02-08) + [2011-02,2011-07) + [2011-07-01,2011-07-02)
	 * @param start
	 * @param end
	 * @return
	 * @throws ParseException
	 */
	private ArrayList<TimeRange> decomposeDayToMonthTimeRangeWithMinusLogik(TimeRange tr) throws ParseException{
		
		ArrayList<TimeRange>  timeRanges = new ArrayList<TimeRange>();
		
		if(tr.type != 1){
			return null;
		}
		
		Date startdate = DATE_FORMAT.parse(tr.getStartDate());
		Date enddate = DATE_FORMAT.parse(tr.getEndDate());
		
		Calendar startDate = Calendar.getInstance();
		startDate.setTime(startdate);
		Calendar endDate = Calendar.getInstance();
		endDate.setTime(enddate);
		Calendar tmpDate = Calendar.getInstance();
		tmpDate.setTime(startdate);
		
		boolean ending = false;
		boolean additive = true;

		if(startDate.get(Calendar.DATE) != 1 && !ending){
			Date startDay = startDate.getTime();
			Date endDay;
			
			// in the same month
			if(startDate.get(Calendar.YEAR)==endDate.get(Calendar.YEAR) && startDate.get(Calendar.MONTH)==endDate.get(Calendar.MONTH)){
				endDay = endDate.getTime();
				ending = true;
			}else {
				// dd.mm.yy -- 01.mm+1.yy
				if(startDate.get(Calendar.DATE) > 10){
					startDate.set(Calendar.DATE,1);
					startDate.add(Calendar.MONTH, 1);
					endDay = startDate.getTime();
					additive = true;
				}
				// -(01.mm.yy -- dd.mm.yy)
				else {
					endDay = startDay;
					startDate.set(Calendar.DATE,1);
					startDay = startDate.getTime();
					additive = false;
				}	
			}
				
			timeRanges.add(new TimeRange(1,startDay, endDay, additive));						
		}
		
		if(startDate.compareTo(endDate)==0){
			ending = true;
		}
		
		if(endDate.get(Calendar.DATE) != 1 && !ending){
			Date startDay;
			Date endDay = endDate.getTime();
			
			//01.mm.yy -- dd.mm.yy
			if(endDate.get(Calendar.DATE) < 20){
				endDay = endDate.getTime();
				endDate.set(Calendar.DATE,1);
				startDay = endDate.getTime();
				additive = true;
			}
			// -(dd.mm.yy -- 01.mm+1.yy)
			else{
				startDay = endDate.getTime();
				endDate.add(Calendar.MONTH, 1);
				endDate.set(Calendar.DATE,1);
				endDay = endDate.getTime();
				additive = false;
			}
			timeRanges.add(new TimeRange(1,startDay, endDay, additive));	
		}
		
		if(startDate.compareTo(endDate)==0){
			ending = true;
		}
		
		if(!ending){
			Date startMonth = startDate.getTime();
			Date endMonth = endDate.getTime();
			timeRanges.add(new TimeRange(2,startMonth, endMonth));	
		}	
		return timeRanges;	
	}

	
	/**
	 * Set columnFamily and qualifier for the given query, which match the current column family structure best.
	 * @param columnFamily
	 * @param qualifier
	 * @param q: query
	 * @return
	 */
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
	
	
	private double[] queryWithScan(byte[] startRow, byte[] endRow,
			byte[][] columnFamily, byte[][] qualifier, int size) throws IOException {
		
    	double result[] = new double[size]; 
    	 Scan scan = new Scan();
    	 
//    	 scan.setLoadColumnFamiliesOnDemand(true);
    	 
    	 scan.setStartRow(startRow);
    	 scan.setStopRow(endRow);
    	 
 		for(int i = 0; i < size; i ++){
			scan.addColumn(columnFamily[i], qualifier[i]);
		}
 		
 		ResultScanner rs = htable.getScanner(scan);
 		int count = 0;
 		for(Result r : rs){
 			count++;
 			
 			TypeRowKey key = new TypeRowKey(r.getRow());
// 			System.out.println(key.toString());
 			
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
//			System.out.println(count + "RowKey: " + rowkey.toString());

 		}
 		
 		return result;

	}
	
	
	
	private double[] queryWithCoproc(final byte[] startRow, final byte[] endRow,
			final byte[][] columnFamily, final byte[][] qualifier, final int size) throws Throwable {

		    	
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
	

	/**
	 * If two byte array are same
	 * @param b1
	 * @param b2
	 * @return
	 */
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
	
	/**
	 * For printing double array
	 * @param result
	 * @return
	 */
	private static String doubleArrayToString (double []result){
		String s = new String();
		s+="[";
		for (int i = 0; i < result.length; i ++){
			s += result[i] +"; ";
		}
		s += "]";
		return s;
	}
	
	/**
	 * Validate the preaggregated data
	 * @param progId
	 * @param startDate
	 * @param endDate
	 * @param q : query
	 * @throws Throwable
	 */
	public void validatePreaggregationCoproc(int progId, String startDate, String endDate, Query q) throws Throwable{
		
		long startTime0 = System.nanoTime();
		double [] result0 = coprocAggregate(q,progId,startDate, endDate, 0);
		long rTime0 = System.nanoTime() - startTime0;
		
//		long startTime1 = System.nanoTime();
//		double [] result1 = coprocAggregate(q,progId,startDate, endDate, 1);
//		long rTime1 = System.nanoTime() - startTime1;
	
		System.out.println("Validate month preaggregation...");
		System.out.println("Time range: " + "[" + startDate +"," +endDate +"]");
		System.out.println("booking_day  : " + doubleArrayToString(result0) +" response time: " + rTime0/1000/1000 +"ms");
//		System.out.println("booking_month: " + doubleArrayToString(result1)+" response time: " + rTime1/1000/1000 +"ms");
	}
	public void validatePreaggregationScan(int progId, String startDate, String endDate, Query q) throws Throwable{
		
		long startTime0 = System.nanoTime();
		double [] result0 = scanAggregate(q,progId,startDate, endDate, 0);
		long rTime0 = System.nanoTime() - startTime0;
		
//		long startTime1 = System.nanoTime();
//		double [] result1 = scanAggregate(q,progId,startDate, endDate, 1);
//		long rTime1 = System.nanoTime() - startTime1;
	
		System.out.println("Validate month preaggregation...");
		System.out.println("Time range: " + "[" + startDate +"," +endDate +"]");
		System.out.println("booking_day  : " + doubleArrayToString(result0) +" response time: " + rTime0/1000/1000 +"ms");
//		System.out.println("booking_month: " + doubleArrayToString(result1)+" response time: " + rTime1/1000/1000 +"ms");
	}
	
	
	public static void main(String [] args) throws Throwable{

		
		int progId = 20;
		BookingTable bt0 = new BookingTable("report00", 0);

		int []items = new int [109];
		for(int i = 0; i < 109; i ++){
			items[i] = i;
		}
		
		Query q = new Query(items);
//		System.out.println("Query: " + q.toString());	
		String startDate = "2012-01-01-00";
		String endDate = "2012-02-07-00";
//		bt0.validatePreaggregationScan(progId, startDate, endDate, q);
//		bt0.validatePreaggregationCoproc(progId, startDate, endDate, q);
//
//		bt0.close();
//		ArrayList<TimeRange> trs = bt0.decomposeMonthTimeRange(startDate,endDate);
//		ArrayList<TimeRange> trs = bt0.decomposeMonthTimeRangeWithMinusLogik(startDate,endDate);
//		for(TimeRange tr : trs){
//			System.out.print(tr.toString() +" ");
//		}
//		System.out.println();
		
//		Date d1 = HOUR_FORMAT.parse("2012-01-01-01");
//		Date d2 = HOUR_FORMAT.parse("2012-05-05-00");
//		TimeRange tr = bt0.buildTimeRange(0, d1, d2);
////		bt0.decomposeTimeRanges(0, tr, true);
////		bt0.decomposeTimeRanges(1, tr, true);
//		bt0.decomposeTimeRanges(2, tr, true);

		double result0[] = bt0.scanPreAggregate(q, progId, startDate, endDate, 0, true);
		System.out.println(" progId: " + progId + "[" + startDate + "," + endDate + "]" + doubleArrayToString(result0));
		double result1[] = bt0.scanPreAggregate(q, progId, startDate, endDate, 1, true);
		System.out.println(" progId: " + progId + "[" + startDate + "," + endDate + "]" + doubleArrayToString(result1));
//		double result2[] = bt2.scanPreAggregate(q, progId, startDate, endDate, true);
//		System.out.println(" progId: " + progId + "[" + startDate + "," + endDate + "]" + doubleArrayToString(result2));
//		double result3[] = bt3.scanPreAggregate(q, progId, startDate, endDate, true);
//		System.out.println(" progId: " + progId + "[" + startDate + "," + endDate + "]" + doubleArrayToString(result3));

		
		
	}	
}