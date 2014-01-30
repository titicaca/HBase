package com.zanox.statistics.hbase.colfamsbuilding.algo.types;

import com.zanox.statistics.hbase.colfamsbuilding.algo.core.Constants;

public class Attribute{
	int id;
	String name;
	

	
	public Attribute(int i){
		
		this.id = i;
		name = getAttributeName(i);
	}
	
	Attribute(int i, String n){
		id = i;
		name = n;
	}
	
	static public String getAttributeName(int id){
		String []items = Constants.attributes.split(", ");
		return items[id];	
	}
	
	public int getID(){
		return id;
	}
	
	public String getName(){
		return name;
	}
	
	public void change(int i, String n){
		id = i;
		name = n;
	}
	
	@Override
	public boolean equals(Object obj)
	{
		 Attribute a = (Attribute)obj;
		 if(a.id == id && a.name.equals(name)){
			 return true;
		 }
		 else return false;
	}
	
	@Override
	 public int hashCode() {
		 return 0;
	 }
	
	@Override
	public String toString(){
		return "id: "+this.id + " name: "+ this.name;
	}
		 	
	        
}