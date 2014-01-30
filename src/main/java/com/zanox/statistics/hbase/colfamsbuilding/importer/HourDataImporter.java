package com.zanox.statistics.hbase.colfamsbuilding.importer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zanox.statistics.hbase.colfamsbuilding.algo.types.ColFam;



/**
 * 
 * @author fangzhou.yang
 *
 */
public class HourDataImporter{
	private static final Logger LOGGER = LoggerFactory.getLogger(HourDataImporter.class);
	private static Configuration conf = null;
	static {
	     conf = HBaseConfiguration.create();
	     conf.set("hbase.zookeeper.quorum", "t-cloudera-01.zanox.com");
	}
	private static HTablePool pool = null;
	
	private static Integer bulkSize = 2097152;
	
	private static HTableInterface htables [] = null;
	
	
	String dataFile;
	String tableNames[];
	String colFamStructureFiles[];
	int numTables;
	
	static final SimpleDateFormat DATE_FORMAT= new SimpleDateFormat("yyyy-MM-dd");
	static final SimpleDateFormat HOUR_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HH");
	static final SimpleDateFormat MONTH_FORMAT = new SimpleDateFormat("yyyy-MM");
	
	static int numProgs = 1000;
	static int numActiveProgs = 100;
	static String startDateString = "2012-01-01";
	static String endDateString = "2014-01-01";
	
	private final int linesPerHour_min = 120;
	private final int linesPerHour_max = 160; //AVG = 140
	private final float RATE_activeLinesPerHour = (float) 0.6; //85; //60%
	private final float RATE_unactiveLinesPerHour = (float) 0.4; //55; //40%
	

	
	Random random = new Random(100);
		
	ArrayList<ArrayList<ColFam>> colfamsList = new ArrayList<ArrayList<ColFam> > ();
	
	
	/**
	 * 
	 * @param dataFileName
	 * @param hTableNames
	 * @param structureFiles
	 */
	HourDataImporter(String dataFileName, String hTableNames[], String structureFiles[]){
		this.dataFile = dataFileName;
		this.tableNames = hTableNames;
		numTables = hTableNames.length;
		colFamStructureFiles = structureFiles;
		htables = new HTableInterface[numTables];
		
		for(int i = 0; i < numTables; i ++){
			ArrayList<ColFam> colfams = new ArrayList<ColFam> ();
			colfamsList.add(colfams);
		}
	}
	
	
	public void setNumProgs(int progs){
		numProgs = progs;
	}
	
	/**
	 * date shoule be the format "YYYY-MM-DD"
	 * @param s
	 */
	public void setDate(String start, String end){
		startDateString = start;
		endDateString = end;
	}
	
	public void setRandomSeed(int seed){
		random = new Random(seed);
	}
	
	void loadColFams() throws IOException{
		for(int i = 0; i < this.numTables; i ++){
			File file = new File(colFamStructureFiles[i]);
			BufferedReader br = new BufferedReader (new FileReader(file));
			int num = Integer.parseInt(br.readLine());
			int no = 0;
			while(br.ready()){
				no++;
				String line = br.readLine();
				String items[] = line.split(" ");
				ColFam c = new ColFam(items, ("cf"+no) );
				colfamsList.get(i).add(c);	
			}
			br.close();
		}
		
	}
	
	
	void importData() throws IOException, ParseException{
		File file = new File(dataFile);
		BufferedReader br = new BufferedReader (new FileReader(file));
		
		pool = new HTablePool(conf, 10);
		for(int i = 0; i < numTables; i ++){
			htables[i] = pool.getTable(tableNames[i]);
			htables[i].setAutoFlush(false);
			htables[i].setWriteBufferSize(bulkSize);
		}
		
		int lineNo = 0;

		Calendar currentDate = Calendar.getInstance();
		currentDate.setTime(this.DATE_FORMAT.parse(startDateString));
		Calendar endDate = Calendar.getInstance();
		endDate.setTime(this.DATE_FORMAT.parse(endDateString));
		
		System.out.println("Data Importing..");

		for(; currentDate.compareTo(endDate) < 0 && br.ready(); currentDate.add(Calendar.HOUR, 1)){
			int randomRecordsNum = random.nextInt(this.linesPerHour_max - this.linesPerHour_min) + this.linesPerHour_min;
			int activeRecordsNum = (int) (randomRecordsNum * this.RATE_activeLinesPerHour);
			int unactiveRecordsNum = randomRecordsNum - activeRecordsNum;
			
			int []activeProgIdList = this.getRandomIndexList(activeRecordsNum, 0, numActiveProgs);
			int []unactiveProgIdList = this.getRandomIndexList(unactiveRecordsNum, numActiveProgs, numProgs);
			
			
			for(int m = 0; m < activeRecordsNum + unactiveRecordsNum; m ++){
				int progId;
				if(m < activeRecordsNum){
					progId = activeProgIdList[m];					
				}else{
					progId = unactiveProgIdList[m - activeRecordsNum];	
				}
				
				String currentTime = HOUR_FORMAT.format(currentDate.getTime());
				TypeRowKey hourRowKey = new TypeRowKey(0, progId, currentTime );
				byte [] rowkey = hourRowKey.getBytes();
				
				br.read();
				String line = br.readLine();
				//System.out.println(line);
				String items[] =line.split(",", -1);
				
				for(int t = 0; t < numTables; t++){
					Put put = new Put(rowkey);
					for(int i = 0; i < items.length; i ++){
						if(items[i].compareTo("") != 0){
							for(ColFam c : colfamsList.get(t)){
								if(c.isAttContained(i)){
									try{
										put.add(Bytes.toBytes(c.getName()), Bytes.toBytes(i), Bytes.toBytes(Float.parseFloat(items[i])));
									}catch(Exception e){
										LOGGER.error("Import data error when inputing element" + items[i] +" \n" + e.toString());
									}
								}
							}					
						}
					}			
					htables[t].put(put);
				}
							
				lineNo ++;
				if(lineNo%10000 == 0) {
					System.out.println("Imported Lines: " + lineNo + "Time: " + hourRowKey.toString() );
				}				
			}
	
		}	
		for(int t = 0; t < numTables; t++){
			htables[t].flushCommits();
			htables[t].close();
		}	
		pool.close();
		br.close();
		System.out.println("Import Successful!");
		
	}
	
	private int [] getRandomIndexList(int num, int minIndex, int maxIndex){
		int []results = new int [num];
		
		ArrayList <Integer> candidates = new ArrayList<Integer>();
		
		for(int i = minIndex; i < maxIndex; i ++){
			candidates.add(i);
		}
		
		for(int i = 0; i < num; i ++){
			int index = random.nextInt(candidates.size());
			results[i] =candidates.get(index);
			candidates.remove(index);
		}
		
		return results;
		
	}
	
	public static void main (String [] args ) throws IOException, ParseException{
		String htableNames [] = {"report00", "report01", "report02", "report03"};
		String path = "results\\structures\\";
		String structuresPath [] = {path+ "s00" , path + "s01", path + "s02", path + "s03"};
		String datasetPath = HourDataImporter.class.getResource("/StorageDataset/dataset").getPath();
		
		HourDataImporter importer = new HourDataImporter(datasetPath, htableNames, structuresPath);
		importer.loadColFams();
		importer.importData();
		
		
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