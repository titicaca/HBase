package com.zanox.statistics.hbase.colfamsbuilding.testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import com.zanox.statistics.hbase.colfamsbuilding.importer.Rowkey;


public class BookingDataImporter {
	private static final Logger LOGGER = LoggerFactory.getLogger(BookingDataImporter.class);

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
	static int numProgs = 1000;
	static String startDate = "2004-01-01";
	static String endDate = "2014-01-01";
	Random random = new Random(100);
		
	ArrayList<ArrayList<ColFam>> colfamsList = new ArrayList<ArrayList<ColFam> > ();
	
	BookingDataImporter(String dataFileName, String hTableNames[], String structureFiles[]){
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
		startDate = start;
		endDate = end;
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
		
		System.out.println("Data Importing..");
		
		//lineNo starts with 1
		while(br.ready()){
			lineNo ++;
			if(lineNo%10000 == 0) {
				System.out.println("Imported Lines: " + lineNo);
			}
			br.read();
			String line = br.readLine();
			//System.out.println(line);
			String items[] =line.split(",", -1);
						
			byte [] rowkey = getRandomRowKey().getBytes();
			
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
			
		}		
		
		for(int t = 0; t < numTables; t++){
			htables[t].flushCommits();
			htables[t].close();
		}
		pool.close();
		System.out.println("Import Successful!");		
	}
	
	public Rowkey getRandomRowKey() throws ParseException {
		int progid = random.nextInt(numProgs);
		String time = getRandomDate();
		return new Rowkey(progid, time);
	}
	
	private String getRandomDate() throws ParseException{
		long start = DATE_FORMAT.parse(startDate).getTime();
		long end = DATE_FORMAT.parse(endDate).getTime();
		long diff = end - start;
		return DATE_FORMAT.format(new Date( (start + (long)(random.nextFloat() * diff))));
	}
	
	public static void  main(String [] args) throws IOException, ParseException{
		
		final String STRUCTURE_FILE00 = "structures//Structure00";
		final String STRUCTURE_FILE01 = "structures//Structure01";
		final String STRUCTURE_FILE02 = "structures//Structure02";
		final String STRUCTURE_FILE03 = "structures//Structure03";
		
		String dataFileName = "dataset";
		String hTableNames [] = { "booking_00", "booking_01", "booking_02", "booking_03" };
		String structureFiles [] = { STRUCTURE_FILE00, STRUCTURE_FILE01, STRUCTURE_FILE02, STRUCTURE_FILE03 };
		
		BookingDataImporter importer = new BookingDataImporter(dataFileName, hTableNames, structureFiles);
		
		importer.loadColFams();
		importer.importData();
		
	}
}