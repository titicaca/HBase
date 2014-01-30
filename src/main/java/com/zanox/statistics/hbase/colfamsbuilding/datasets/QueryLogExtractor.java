package com.zanox.statistics.hbase.colfamsbuilding.datasets;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Extract useful queries with ProgId and TimeRange from the original query log file
 * the output file format:
 * delimiter: ","
 * [0--108]: 0/1, requesting columns bit map;   0-this column is not requested, otherwise it is requested.
 * [109]: startDate
 * [110]: endData
 * [111....]: ProgId
 * 
 * @author fangzhou.yang
 *
 */
public class QueryLogExtractor{
	public String inputFileName;
	public String symbolString;
	public String outputFileName;
	public String [] attributes; 
	public int daysThreshold = 0;
	
	SimpleDateFormat DATE_FORMAT_SHORT = new SimpleDateFormat("yyyyMMdd");
	SimpleDateFormat DATE_FORMAT_LONG = new SimpleDateFormat("yyyy-MM-dd");
	
	public QueryLogExtractor (String inputFileName, String symbolString, String outputFileName){
		this.inputFileName = inputFileName;
		this.symbolString = symbolString;
		this.outputFileName = outputFileName;
		this.attributes = Constants.attributes.split(", ");
		
		for (String i : this.attributes){
			System.out.println(i);
		}
	}
	
	
	public long extractBigQueryWithTimeRange() throws ParseException{
		long rowcount =0;
		try{		
			File file = new File(inputFileName);		
			File outputFile = new File(outputFileName);
			
			System.out.println("input file: " + this.inputFileName +" output file: " + this.outputFileName);
			System.out.println("start extracting the file..");
			BufferedReader br = new BufferedReader(new FileReader(file));
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile,false));
			

			while(br.ready()){
				String line = br.readLine();			
				int pos = line.indexOf(symbolString);
				
				int pos_startDate = line.indexOf("StartDate");
				int pos_stopDate = line.indexOf("StopDate");
				
				if(pos != -1 && pos_startDate != -1 && pos_stopDate != -1){
					rowcount++;
					
					String subStartDateLine = line.substring(pos_startDate);
					String startDate = subStartDateLine.substring(subStartDateLine.indexOf(" ")+1, subStartDateLine.indexOf(")"));
					
					String subStopDateLine = line.substring(pos_stopDate);
					String stopDate = subStopDateLine.substring(subStopDateLine.indexOf(" ")+1, subStopDateLine.indexOf(")"));

					String subline = line.substring(pos);
					String valueStr = subline.substring(subline.indexOf("[")+1, subline.indexOf("]"));
										
					String []valueItems = valueStr.split(", ");
					
					String result = "";
					
					for (int i = 0; i < attributes.length; i++){
						String att = attributes[i];
						boolean match = false;
						for ( String s : valueItems){
							if (att.compareTo(s) == 0){
								match = true;
//								System.out.print(s + " ");
								break;
							}
						}
						if ( match){
							result = result + "1";
						}
						else{
							result = result + "0";
						}
						
						if ( i != (attributes.length-1) ){
							result = result +",";
						}
						else{
							result = result + "," + startDate + "," + stopDate +"\n";
						}
					}
//					System.out.print(rowcount + "\n");

					//days interval threshold
					if(this.isPassTreashold(this.calculateTimeInterval(startDate, stopDate))){
						bw.write(result);							
					}else{
						rowcount --;
					}

					if(rowcount % 10000 == 0){
						System.out.print(rowcount + " " + result);
					}
				}
			}
			
			bw.close();
			br.close();
		}catch(FileNotFoundException e){
			e.printStackTrace();
		}catch (IOException e){
			e.printStackTrace();
		}
		
		return rowcount;
	}
	
	public long extractBigQueryWithTimeRangeAndProgId() throws ParseException{
		long rowcount =0;
		long count = 0;
		String line = null;
		try{		
			File file = new File(inputFileName);		
			File outputFile = new File(outputFileName);
			
			System.out.println("input file: " + this.inputFileName +" output file: " + this.outputFileName);
			System.out.println("start extracting the file..");
			BufferedReader br = new BufferedReader(new FileReader(file));
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile,false));
			

			while(br.ready()){
				line = br.readLine();			
				int pos = line.indexOf(symbolString);
				
				int pos_progId = line.indexOf("ContentFilters PROG_ID"); 
				
				int pos_startDate = line.indexOf("StartDate");
				int pos_stopDate = line.indexOf("StopDate");
				
				if(pos != -1 && pos_startDate != -1 && pos_stopDate != -1 && pos_progId != -1){
					rowcount++;
					
					String subProgIdLine = line.substring(pos_progId);
					String progIds = subProgIdLine.substring(subProgIdLine.indexOf("[")+1, subProgIdLine.indexOf("]")).replaceAll(" ", "");
					
					
					String subStartDateLine = line.substring(pos_startDate);
					String startDate = subStartDateLine.substring(subStartDateLine.indexOf(" ")+1, subStartDateLine.indexOf(")"));
					
					String subStopDateLine = line.substring(pos_stopDate);
					String stopDate = subStopDateLine.substring(subStopDateLine.indexOf(" ")+1, subStopDateLine.indexOf(")"));

					String subline = line.substring(pos);
					String valueStr = subline.substring(subline.indexOf("[")+1, subline.indexOf("]"));
										
					String []valueItems = valueStr.split(", ");
					
					String result = "";
					
					for (int i = 0; i < attributes.length; i++){
						String att = attributes[i];
						boolean match = false;
						for ( String s : valueItems){
							if (att.compareTo(s) == 0){
								match = true;
//								System.out.print(s + " ");
								break;
							}
						}
						if ( match){
							result = result + "1";
						}
						else{
							result = result + "0";
						}
						
						if ( i != (attributes.length-1) ){
							result = result +",";
						}
						else{
							result = result + "," + startDate + "," + stopDate + "," + progIds + "\n";
						}
					}
//					System.out.print(rowcount + "\n");

					
					//days interval threshold
					if(this.isPassTreashold(this.calculateTimeInterval(startDate, stopDate))){
						bw.write(result);		
						count ++;
					}

					if(rowcount % 10000 == 0){
						System.out.print(rowcount + " " + result);
					}
				}
			}
			
			bw.close();
			br.close();
		}catch(FileNotFoundException e){
			e.printStackTrace();
		}catch (IOException e){
			System.out.println(line);
			e.printStackTrace();
		}
		
		return count;
	}
	
	public long extractBigQuery(){
		long rowcount =0;
		try{		
			File file = new File(inputFileName);		
			File outputFile = new File(outputFileName);
			
			System.out.println("input file: " + this.inputFileName +" output file: " + this.outputFileName);
			System.out.println("start extracting the file..");
			BufferedReader br = new BufferedReader(new FileReader(file));
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile,false));
			

			while(br.ready()){
				String line = br.readLine();			
				int pos = line.indexOf(symbolString);

				
				if(pos != -1 ){
					rowcount++;
					
					String subline = line.substring(pos);
					String valueStr = subline.substring(subline.indexOf("[")+1, subline.indexOf("]"));
										
					String []valueItems = valueStr.split(", ");
					
					String result = "";
					
					for (int i = 0; i < attributes.length; i++){
						String att = attributes[i];
						boolean match = false;
						for ( String s : valueItems){
							if (att.compareTo(s) == 0){
								match = true;
//								System.out.print(s + " ");
								break;
							}
						}
						if ( match){
							result = result + "1";
						}
						else{
							result = result + "0";
						}
						
						if ( i == (attributes.length-1) ){
							result = result + "\n";
						}
					}

					bw.write(result);	

					if(rowcount % 10000 == 0){
						System.out.print(rowcount + " " + result);
					}
				}
			}
			
			bw.close();
			br.close();
		}catch(FileNotFoundException e){
			e.printStackTrace();
		}catch (IOException e){
			e.printStackTrace();
		}
		
		return rowcount;
	}
	
	private boolean isPassTreashold(int days){
		if(days <= 0){
			return false;
		}else{
			if(days > this.daysThreshold){
				return true;
			}
			else{
				return false;
			}
		}
	}
	
	private int calculateTimeInterval(String startDate, String endDate) throws ParseException{
		Date start = DATE_FORMAT_SHORT.parse(startDate);
		Date end = DATE_FORMAT_SHORT.parse(endDate);
		long diff = end.getTime() - start.getTime();
		int days = (int) ((diff+1) / (1000 * 60 * 60 * 24));
		return days;
	}
	
	
	public static void main (String args[]) throws ParseException{
		String input = QueryLogExtractor.class.getResource("/OriginalUserLogQuery/merge-BOOKING_SEARCH-0_1").getPath();
		String output =  "datasets\\QueryDataset\\FilteredQueries";
		
		QueryLogExtractor ve = new QueryLogExtractor(input, "Valuers", output);
		ve.daysThreshold = 0;
		
		//TODO
//		System.out.println(ve.calculateTimeInterval("20120321","20120326"));
		long count = ve.extractBigQueryWithTimeRangeAndProgId();
		System.out.println("rows: " +count);
	
	}
}