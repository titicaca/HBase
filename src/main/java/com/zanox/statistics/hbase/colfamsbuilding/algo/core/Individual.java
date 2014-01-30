package com.zanox.statistics.hbase.colfamsbuilding.algo.core;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import com.zanox.statistics.hbase.colfamsbuilding.algo.types.Attribute;
import com.zanox.statistics.hbase.colfamsbuilding.algo.types.ColFam;
import com.zanox.statistics.hbase.colfamsbuilding.algo.types.Query;

public class Individual{
	//to be calculated
	float skew;
	float ColFamConsume;
	float cfLoaded[];
	int sumAtts;
	float cost;
	int id;
	float loadBalance;
	float duplicateRate;
	
	//to be initialized
	int colFamNum;
	ArrayList<ColFam> colfams = new ArrayList<ColFam>();
	
	Individual(int num, Random r, int ID, int duplicate){
		colFamNum = num;
		for(int i = 0; i < num; i++){
			ColFam cf = new ColFam();
			colfams.add(cf);
		}
		for(int d = 0; d < duplicate; d++){
			for(int i = 0; i < Constants.NUM_AttributeSize; i++){
				int tmp = r.nextInt(num);
				Attribute a = new Attribute(i);
				colfams.get(tmp).addAtt(a);
			}
		}
		this.id = ID;
	
	}
	
	
	Individual(Individual i, int ID){
		this.colFamNum = i.colFamNum;
		this.skew = i.skew;
		this.ColFamConsume = i.ColFamConsume;
		this.sumAtts = i.sumAtts;
		this.cost = i.cost;
		this.duplicateRate = i.duplicateRate;
		this.loadBalance = i.loadBalance;
		this.cfLoaded = new float [i.colFamNum];
		for(int m = 0; m < i.colFamNum; m++){
			this.cfLoaded[m] = i.cfLoaded[m]; 
		}
		for(ColFam c : i.colfams){
			colfams.add(new ColFam(c));
		}
		id = ID;
	}
	
	public Individual(int id) {
		this.id = id;
		colFamNum = 0;
	}
	
	
	//all the factors are scaled to [0,1]
	public float calculateCost(){
		float result = 0;
		result =  Constants.FACTOR_ColFamNum * 1/colFamNum 
					//+ Constants.FACTOR_Duplicate * calculateSumAtts() / Constants.colFamNum_MAX / Constants.NUM_AttributeSize
					+ Constants.FACTOR_Duplicate * calculateDuplicate_WithWeight()/ this.colFamNum
					//+ Constants.FACTOR_Skew * calculateSkew() 
					+ Constants.FACTOR_Skew * calculateSkew_WithWeight()
					//+ Constants.FACTOR_ColFamConsume * ((float)calculateColFamConsume()/Constants.colFamNum_MAX/Constants.queries.size())
					+ Constants.FACTOR_ColFamConsume * (calculateColFamConsume_FromQueryPrototypes()/colFamNum)
					+ Constants.FACTOR_LoadBalance * calculateLoadBalancing();
		//this.cost = result;
		this.cost = result / (Constants.FACTOR_ColFamConsume + Constants.FACTOR_ColFamNum + Constants.FACTOR_Duplicate + Constants.FACTOR_LoadBalance + Constants.FACTOR_Skew);
		
		return result;		
	}
	
	//it should be used after the function ColFamConsume;
	private float calculateLoadBalancing(){
		float result = 0;
		float tempSum = 0;
		
		float avg = this.ColFamConsume / colFamNum;
		for(int i = 0; i < colFamNum; i++){
			tempSum += Math.pow(cfLoaded[i] - avg, 2);
		}
//TODO
		result = (float)Math.sqrt(tempSum / colFamNum) * 2;
		
		loadBalance = result;
		return result;
	}
	

	
	private float calculateSkew(){
		float tempSum = 0;
		float avgAttNum = calculateSumAtts() / colFamNum;
		for(int i = 0; i < colFamNum ; i++){
			tempSum += Math.pow(colfams.get(i).getAttsNum() - avgAttNum,2);
		}
		float result =(float) Math.sqrt(tempSum / colFamNum) / (Constants.NUM_AttributeSize -1) *2 ;
		
		this.skew = result;
		
		return result;
	}
	
	private float calculateSkew_WithWeight(){
		float sum = 0;
		calculateSumAtts();
		float values[] = new float[colFamNum];
		for(int i = 0; i < colFamNum ; i++){
			for(int j = 0; j < colfams.get(i).getAttsNum(); j++){
				int id = colfams.get(i).getAttByIndex(j).getID();
				values[i] += Constants.weight.getWeightByAttId(id); 
				sum += Constants.weight.getWeightByAttId(id);
			}
		}
		float avg = sum / colFamNum;
		
		float tempSum = 0;
		for(int i = 0; i < colFamNum ; i++){
			tempSum += Math.pow(values[i] - avg,2);
		}
		float result =(float) Math.sqrt(tempSum / colFamNum) * 2 ;
		
		this.skew = result;
		
//		System.out.println("calculateSkew_WithWeight: " + result);
		return result;
		
	}
	
	private float calculateDuplicate_WithWeight(){
		float sum = 0;
		calculateSumAtts();
		float values[] = new float[colFamNum];
		for(int i = 0; i < colFamNum ; i++){
			for(int j = 0; j < colfams.get(i).getAttsNum(); j++){
				int id = colfams.get(i).getAttByIndex(j).getID();
				values[i] += Constants.weight.getWeightByAttId(id); 
				sum += Constants.weight.getWeightByAttId(id);
			}
		}
		this.duplicateRate = sum;
//		System.out.println("calculateDuplicate_WithWeight: " + sum);

		return sum;
		
	}
	
	private float calculateColFamConsume_FromQueryPrototypes(){
		float result = 0;	
		this.cfLoaded = new float [colFamNum];
		for(int i = 0; i < DataQueries.queries.size(); i++){
			Query q = new Query(DataQueries.queries.get(i));
			while(q.getAttNum()>0){				
				//choose the colfam which has the max match atts
				int maxMatchedAtts = 0;
				int maxCf = 0;
				int matchedAtts = 0;
				for(int m = 0; m < colFamNum; m++){
					ColFam c = colfams.get(m);
					for(int x = 0; x < q.getAttNum(); x++ ){
						for(int y = 0; y < c.getAttsNum(); y++){
							if(q.getAttByIndex(x).getID() == c.getAttByIndex(y).getID()){
								matchedAtts ++;
								break;
							}
						}
					}					
					if(m == 0){
						maxMatchedAtts = matchedAtts;
						maxCf = 0;
					}
					else{
						if(maxMatchedAtts < matchedAtts){
							maxCf = m;
							maxMatchedAtts = matchedAtts;
						}
					}					
				}
				if(maxMatchedAtts == 0 )
				{
					System.out.println("Error Happened!");
					q.printQuery();
					this.printIndividual();
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				//delete the matched atts in the query
				ColFam c = colfams.get(maxCf);
				for(int y = 0; y < c.getAttsNum(); y++){
					for(int x = 0; x < q.getAttNum(); x++ ){
						if(q.getAttByIndex(x).getID()== c.getAttByIndex(y).getID()){
							q.removeAttByIndex(x);
							break;
						}
					}
				}
				cfLoaded[maxCf] += q.frequency;
				result += q.frequency;
			}
		}
		this.ColFamConsume = result;
//		System.out.println("calculateColFamConsume_FromQueryPrototypes: " + result);
		return result;
	}
	
	//it should be used after the function calculateSumAtts()
	private int calculateColFamConsume(){
		int result = 0;	
		this.cfLoaded = new float [colFamNum];
		for(int i = 0; i < DataQueries.queries.size(); i++){
			Query q = new Query(DataQueries.queries.get(i));
			//System.out.println(i);
			while(q.getAttNum()>0){				
				//choose the colfam which has the max match atts
				int maxMatchedAtts = 0;
				int maxCf = 0;
				int matchedAtts = 0;
				for(int m = 0; m < colFamNum; m++){
					ColFam c = colfams.get(m);
					for(int x = 0; x < q.getAttNum(); x++ ){
						for(int y = 0; y < c.getAttsNum(); y++){
							if(q.getAttByIndex(x).getID() == c.getAttByIndex(y).getID()){
								matchedAtts ++;
								break;
							}
						}
					}					
					if(m == 0){
						maxMatchedAtts = matchedAtts;
						maxCf = 0;
					}
					else{
						if(maxMatchedAtts < matchedAtts){
							maxCf = m;
							maxMatchedAtts = matchedAtts;
						}
					}					
				}
				if(maxMatchedAtts == 0 )
				{
					System.out.println("Error Happened!");
					q.printQuery();
					this.printIndividual();
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				//delete the matched atts in the query
				ColFam c = colfams.get(maxCf);
				for(int y = 0; y < c.getAttsNum(); y++){
					for(int x = 0; x < q.getAttNum(); x++ ){
						if(q.getAttByIndex(x).getID() == c.getAttByIndex(y).getID()){
							q.removeAttByIndex(x);
							break;
						}
					}
				}
				cfLoaded[maxCf] ++;
				result ++;
			}
		}
		this.ColFamConsume = ((float)result) / DataQueries.queries.size();
		return result;
	}
	
	private int calculateSumAtts(){
		int count = 0;
		for(ColFam c : colfams){
			count+= c.getAttsNum();
		}
		this.sumAtts = count;
		return count;
	}
	
	//cross over between two different Column Families (pos1 po2 included)
	public boolean crossOver (int cf1, int cf2, int pos1, int pos2){
		if(cf1 != cf2 && cf1 < colFamNum && cf2 < colFamNum){
			ColFam colfam1 = colfams.get(cf1);
			ColFam colfam2 = colfams.get(cf2);
			//colfams.remove(cf1);
			//colfams.remove(cf2);
			
			ArrayList<Attribute> tmplist1 = new ArrayList<Attribute>();
			ArrayList<Attribute> tmplist2 = new ArrayList<Attribute>();

			tmplist1 = colfam1.cutSubAttList(pos1);
			tmplist2 = colfam2.cutSubAttList(pos2);
			
			colfam1.addAttList(tmplist2);
			colfam2.addAttList(tmplist1);
			
			//colfams.add(colfam1);
			//colfams.add(colfam2);
			
			return true;
			
		}
		else{
			return false;
		}
	}

	
	public boolean crossOver2 (int cf1, int cf2, int pos11, int pos12, int pos21, int pos22){
		if(cf1 != cf2 && cf1 < colFamNum && cf2 < colFamNum  && pos11 > 0 && pos21 > 0 && pos12 > 0 && pos22 > 0){
			
			ColFam colfam1 = colfams.get(cf1);
			ColFam colfam2 = colfams.get(cf2);
			//colfams.remove(cf1);
			//colfams.remove(cf2);
			
			ArrayList<Attribute> tmplist11 = new ArrayList<Attribute>();
			ArrayList<Attribute> tmplist12 = new ArrayList<Attribute>();
			ArrayList<Attribute> tmplist21 = new ArrayList<Attribute>();
			ArrayList<Attribute> tmplist22 = new ArrayList<Attribute>();
			
			
			tmplist11 = colfam1.cutPrefixAttList(pos11);
			tmplist21 = colfam2.cutPrefixAttList(pos21);
			tmplist12 = colfam1.cutSuffixAttList(pos12);
			tmplist22 = colfam2.cutSuffixAttList(pos22);
			
			/*if(tmplist11 == null || tmplist21 == null || tmplist12 == null || tmplist22 == null){
				System.out.println(colfam1.attsNum + " " + colfam2.attsNum);
				System.out.println(pos11 +" " + pos21 + " " + pos12 + " " + pos22);
			}*/
			
			colfam1.addPrefixAttList(tmplist21);
			colfam2.addPrefixAttList(tmplist11);
			
			colfam1.addAttList(tmplist22);
			colfam2.addAttList(tmplist12);
			
			//colfams.add(colfam1);
			//colfams.add(colfam2);
			
			return true;		
		}
		else{
			return false;
		}
	}
	
	
	
	//split one ColFam to two ColFams, with a random point
	public boolean splitUp(int cf, int pos){
		if(colFamNum < Constants.colFamNum_MAX){
			ColFam colfam = colfams.get(cf);
			ArrayList<Attribute> tmplist = new ArrayList<Attribute>();
			tmplist = colfam.cutSubAttList(pos);
			
			ColFam newColfam = new ColFam(tmplist);
			colfams.add(newColfam);
			colFamNum++;
			return true;
		}
		else {
			return false;
		}
	}
	
	//merge two ColFams into one big ColFam
	public boolean merge(int cf1, int cf2){
		if(colFamNum>2 && cf1 != cf2){
			ColFam colfam1 = colfams.get(cf1);
			ColFam colfam2 = colfams.get(cf2);
			
			colfam1.addAttList(new ArrayList<Attribute>(colfam2.getAtts()));
			
			colfams.remove(cf2);
			colFamNum--;
			return true;
		}
		else{
			return false;
		}
	}
	
	public boolean isDuplicate(int attID){
		int count = 0;
		for(ColFam c : colfams){
			if(c.isExist(attID)){
				count ++ ;
			}
		}
		
		if(count >= 2){
			return true;
		}
		else{
			return false;
		}
	}
	
	public boolean isExist(int attID){
		for(ColFam c : colfams){
			if(c.isExist(attID)){
				return true;
			}
		}
		return false;
	}
	
	public void fillUp(Random rand, int index1, int index2){
		if(!(index1 < index2)){
			return;
		}
		for(int i = 0; i < Constants.NUM_AttributeSize; i++){
			if(!isExist(i)){
				int m = rand.nextInt(index2-index1) + index1;
				colfams.get(m).addAtt(new Attribute(i));
			}
		}
	}
	
	public float getCost(){
		return this.cost;
	}
	
	public void printIndividual(){
		System.out.println("----Individual " + id +" ----");
		System.out.println("Cost: " + cost);
		System.out.println("needed ColFams: " + ColFamConsume);
		System.out.println("sumAtts: " + sumAtts);
		//System.out.println("duplicate ratio: " + (float)sumAtts / (float)Constants.NUM_AttributeSize);
		System.out.println("duplicate ratio: " + this.duplicateRate);
		System.out.println("skew: " + skew);
		System.out.println("load balance: " + this.loadBalance );
		System.out.println("Column Families: ");
		for(ColFam c : colfams){
			c.printColFam();
		}
		System.out.println("-------------");
	}
	
	public void printLog(int count) throws IOException{
		Log l = new Log(Constants.LOG_FILE_NAME);
		l.start();
		l.printLogln("-----------Individual: " + count +" -----------------");
		l.printLogln("ID: " + id);
		l.printLogln("Cost: " + cost);
		l.printLogln("needed ColFams: " + ColFamConsume);
		l.printLogln("sumAtts: " + sumAtts);
		//l.printLogln("duplicate ratio: " + (float)sumAtts / (float)Constants.NUM_AttributeSize);
		l.printLogln("duplicate ratio: " + this.duplicateRate);
		l.printLogln("skew: " + skew);
		l.printLogln("load balance: " + this.loadBalance );
		l.printLogln("Column Families: ");
		l.printLogln(this.colFamNum);
		for(ColFam c : colfams){
			l.printLog(c.getAttsNum() + ";");
			for ( Attribute a : c.getAtts()){
				l.printLog(a.getID() + " ");
			}
			l.printLog("\n");
		}
		
		l.printLogln("Load Frequency for each ColFam: ");
		for(int i = 0; i < this.colFamNum; i ++){
			l.printLogln((float)this.cfLoaded[i]/this.ColFamConsume);
		}
		//l.printLog("\n");
		l.end();
	}


	public void printEvolutionLog(int count) throws IOException {
		Constants.eLog.printLogln("-----------Individual: " + count +" -----------------");
		Constants.eLog.printLogln("ID: " + id);
		Constants.eLog.printLogln("Cost: " + cost);
		Constants.eLog.printLogln("needed ColFams: " + ColFamConsume);
		Constants.eLog.printLogln("sumAtts: " + sumAtts);
		//Constants.eLog.printLogln("duplicate ratio: " + (float)sumAtts / (float)Constants.NUM_AttributeSize);
		Constants.eLog.printLogln("duplicate ratio: " + this.duplicateRate);
		Constants.eLog.printLogln("skew: " + skew);
		Constants.eLog.printLogln("load balance: " + this.loadBalance );
		Constants.eLog.printLogln("Column Families: ");
		Constants.eLog.printLogln(this.colFamNum);
		for(ColFam c : colfams){
			Constants.eLog.printLog(c.getAttsNum() + ";");
			for ( Attribute a : c.getAtts()){
				Constants.eLog.printLog(a.getID() + " ");
			}
			Constants.eLog.printLog("\n");
		}

		Constants.eLog.printLogln("Load Frequency for each ColFam: ");
		for(int i = 0; i < this.colFamNum; i ++){
			Constants.eLog.printLogln((float)this.cfLoaded[i]/this.ColFamConsume);
		}
		
	}
	
	@Override
	public String toString(){
		String s = new String();
		s = "Individual ID: " + id + "\nCost: " + cost + "\nneeded ColFams: " + ColFamConsume + "\nsumAtts: " + sumAtts +"\nduplicate ratio: " + duplicateRate + "\nskew " + skew 
		+ "\nload balance " + loadBalance + "\nColumn Families: " + colFamNum +"\n";
		for(ColFam c : colfams){
			s += (c.getAttsNum() + "; ");
			for ( Attribute a : c.getAtts()){
				s +=  (a.getID() + " ");
			}
			s += ("\n");
		}
		return s;
	}
	
}