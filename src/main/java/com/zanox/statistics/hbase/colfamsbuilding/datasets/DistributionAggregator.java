package com.zanox.statistics.hbase.colfamsbuilding.datasets;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.TreeMap;


/**
 * Extract time range from input log file to output file, to see the time range distribution
 * @author fangzhou.yang
 *
 */
public class DistributionAggregator{
	
	final int startDateIndex = 109;
	final int endDateIndex = 110;
	final int progsStartIndex = 111;
	
	final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");
	
	
	public void progsAggregate(String inputFile, String outputFile) throws IOException{
		System.out.println("Extracting progs from the log file: " + inputFile  + " to File: " + outputFile);
		TreeMap<Integer, Long> progMap = new TreeMap <Integer, Long>();
		
		long rowcount =0;
		BufferedReader br = new BufferedReader ( new FileReader(inputFile));
		BufferedWriter bw = new BufferedWriter ( new FileWriter (outputFile, false));
		
		while(br.ready()){
			String line = br.readLine();
			String items[] = line.split(",");
			
			for(int i = this.progsStartIndex; i < items.length; i ++){
				int progId = Integer.parseInt(items[i]);
				long lastValue = (Long) progMap.get(progId) == null ? 0 : progMap.get(progId);
				progMap.put(progId, lastValue + 1);
				
			}
			
			rowcount++;
		}
		
		System.out.println("Count:" + rowcount);
		
		for(Integer key : progMap.keySet()){
			String result = key.toString() + " " + progMap.get(key).toString() + "\n";
			bw.write(result);
		}
		
		bw.flush();
		bw.close();
		br.close();
	}
	
	public void timeRangeAggregate(String inputFile, String outputFile) throws IOException, ParseException{
		System.out.println("Extracting time range from the log file: " + inputFile  + " to File: " + outputFile);
		
		TreeMap<Integer, Long> timeIntervalMap = new TreeMap <Integer, Long>();
		
		long rowcount =0;
		BufferedReader br = new BufferedReader ( new FileReader(inputFile));
		BufferedWriter bw = new BufferedWriter ( new FileWriter (outputFile, false));
		
		while(br.ready()){
			String line = br.readLine();
			
			String items[] = line.split(",");
			
			String startDate = items[this.startDateIndex];
			String endDate = items[this.endDateIndex];
			
			int days = calculateTimeInterval(startDate, endDate);
			
//			if(days==6){
//				System.out.println("[" + startDate + "," + endDate + "]");
//			}
			
			long lastValue = (Long) (timeIntervalMap.get(days) == null ? 0 : timeIntervalMap.get(days));
			
			timeIntervalMap.put(days, lastValue + 1);
			rowcount++;
		}
		
		System.out.println("Count:" + rowcount);
		
		for(Integer key : timeIntervalMap.keySet()){
			String result = key.toString() + " " + timeIntervalMap.get(key).toString() + "\n";
			bw.write(result);
		}
		
		bw.flush();
		bw.close();
		br.close();
	}
	
	private int calculateTimeInterval(String startDate, String endDate) throws ParseException{
		Date start = DATE_FORMAT.parse(startDate);
		Date end = DATE_FORMAT.parse(endDate);
		long diff = end.getTime() - start.getTime();
		int days = (int) ((diff+1) / (1000 * 60 * 60 * 24));
		return days;
	}
	
	public static void main (String [] args) throws IOException, ParseException{
		DistributionAggregator extractor = new DistributionAggregator ();
//		extractor.timeRangeAggregate("datasets//QueryDataSet//FilteredQueries", "datasets//QueryDataset//TimeRangeDistribution");
//		extractor.progsAggregate("datasets//QueryDataSet//FilteredQueries", "datasets//QueryDataset//ProgIdDistribution");
		
		extractor.progsAggregate("datasets//LearningDataSets//Testing_Query", "datasets//LearningDataSets//Testing_ProgsDistribution");

	}
}