package com.zanox.statistics.hbase.colfamsbuilding.algo.types;

import java.util.ArrayList;


public class ColFam{
	
	String name = new String();	
	ArrayList<Attribute> atts = new ArrayList<Attribute>();
	
	public ColFam(String items[], String colFamName){
		for(int i = 0; i < items.length; i ++) {
			atts.add(new Attribute(Integer.parseInt(items[i])));
		}
		this.name = colFamName;
	}
	
	public ColFam(){
	}
	
	public ColFam(ColFam c){
		for(Attribute a : c.atts){
			atts.add(new Attribute(a.id));
		}
	}
	
	public ColFam(ArrayList<Attribute> attributes){
		for(Attribute a : attributes){
			atts.add(new Attribute(a.id));
		}
	}
	

	public String getName(){
		return name;
	}
	
	public ArrayList<Attribute> getAtts(){
		return atts;
	}
	
	public boolean isAttContained(int id){
		for(Attribute i : atts){
			if(i.id == id ){
				return true;
			}
		}
		return false;
	}	
	
	public Attribute getAttByIndex(int i){
		if(i < this.atts.size()){
			return this.atts.get(i);
		}else{
			return null;
		}
	}
	
	public boolean addAtt(Attribute a){
		if(!isExist(a.id))
		{	
			atts.add(a);
			return true;
		}
		return false;
	}
	
	public int getAttsNum(){
		return this.atts.size();
	}
	
	public void printColFam(){
		for ( Attribute a : atts){
			System.out.print(a.id + " ");
		}
		System.out.print("\n");
	}
	
	public boolean removeAtt (Attribute a){
		
		for(int i = 0; i < atts.size(); i++){
			if(a.id == atts.get(i).id){
				atts.remove(i);
				return true;
			}
		}	
		return false;	
	}
	
	public boolean removeAttByIndex(int i){
		if(i < this.atts.size()){
			this.atts.remove(i);
			return true;
		}else{
			return false;
		}
	}
	
	public int addPrefixAttList (ArrayList<Attribute> list){
		int count = 0;
		for (Attribute a:list){
			if(!isExist(a.id)){
				atts.add(count, new Attribute(a.id));
				count++;
			}
		}
		return count;
	}
	
	public int addAttList (ArrayList<Attribute> list){
		int count = 0;
		for(Attribute a:list){
			if(!isExist(a.id)){
				atts.add(new Attribute(a.id));
				count++;
			}
		}
		return count;
	}
	
	//return [pos,end]
	public ArrayList<Attribute> cutSubAttList (int pos){
		if(pos < atts.size()){
			ArrayList<Attribute> sublist = new ArrayList<Attribute>();
			for(int i = pos; i < atts.size(); i++) {
				sublist.add(new Attribute(atts.get(i).id));
			}
			for(int i = pos; i < atts.size(); ) {
				atts.remove(i);
			}
			return sublist;
		}
		else{
			return null;
		}
	}
	
	//return [end-pos, end]
	public ArrayList<Attribute> cutSuffixAttList (int pos){
		if(pos < atts.size()){
			pos = atts.size() - pos;
			ArrayList<Attribute> sublist = new ArrayList<Attribute>();
			for(int i = pos; i < atts.size(); i++) {
				sublist.add(new Attribute(atts.get(i).id));
			}
			for(int i = pos; i < atts.size(); ) {
				atts.remove(i);
			}
			return sublist;
		}
		else{
			return null;
		}
	}
	
	//return [0,pos-1) exclude pos!
	public ArrayList<Attribute> cutPrefixAttList (int pos){
		if(0 < pos && pos < atts.size()){
			ArrayList<Attribute> sublist = new ArrayList<Attribute>();
			for(int i = 0; i < pos; i++) {
				sublist.add(new Attribute(atts.get(i).id));
			}
			for(int count = 0; count < pos; count ++ ) {
				atts.remove(0);
			}
			return sublist;
		}
		else{
			System.err.println("return NULL ERROR!");
			return null;
		}
	}
	
	
	public boolean isExist(int attID){
		for (Attribute a : atts){
			if(a.id == attID){
				return true;
			}
		}
		
		return false;
	}
	
	public boolean isContainedSubAtts(ArrayList<Attribute> sublist){
		int count = 0;
		for(Attribute sub:sublist){
			for(Attribute a : atts){
				if(a.id == sub.id){
					count++;
					break;
				}
			}
		}
		if(count == sublist.size()){
			return true;
		}
		else{
			return false;
		}
	}	
}