package com.redislabs;

import java.util.Iterator;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class PipelineTest {
	Jedis jedis = null;
	
	public PipelineTest(String host, int port) {
		jedis = new Jedis(host, port);	
	}
	
	public void putData()
	{
		String [] keyValues = new String[10];
		int pos = 0;
		for ( int i = 1; i < 6; i++ )
        {    
        	keyValues[pos++] = "key" + i;
        	keyValues[pos++] = "val" + i;
        }
        jedis.mset(keyValues);
	}
	
	public void putDataPipelined()
	{
		Pipeline p = jedis.pipelined();
        for ( int i = 1; i < 6; i++ )
        {    
        	p.set("key" + i, "val" + i);
        }
        p.sync();
	}
	
	public void getData()
    {
		String [] keys = new String[5];
        for ( int i = 0; i < 5; i++ )
        {   
        	keys[i] = "key" + (i + 1);
        }
        
        List<String> valueList = jedis.mget(keys);
        Iterator<String> iter = valueList.iterator();
        while (iter.hasNext())
        {
        	System.out.println(iter.next());
        }
        
        jedis.del(keys);
    }
	
    public void getDataPipelined()
    {
    	Pipeline p = jedis.pipelined();
        for ( int i = 1; i < 6; i++ )
        {    
        	p.get("key" + i);
        	p.del("key" + i);
        }
        
        List<Object> valueList = p.syncAndReturnAll();
        Iterator<Object> iter = valueList.iterator();
        while (iter.hasNext())
        {
        	System.out.println(iter.next());
        	iter.next();
        }
    }
	
	public static void main(String[] args) {
		PipelineTest test = new PipelineTest("localhost", 6379);
		test.putData();
		test.getData();
		test.putDataPipelined();
		test.getDataPipelined();
	}

}
