package com.zanox.statistics.hbase.colfamsbuilding.tools;

import java.io.Serializable;

import org.apache.commons.lang.SerializationUtils;


public class ObjectSerialization{
	
	public static byte [] writeObjectToBytes(Object o){
		byte[] data = SerializationUtils.serialize((Serializable) o);
		return data;		
	}
	
	public static Object readObjectFromBytes(byte [] data){
		return SerializationUtils.deserialize(data);
	}
}