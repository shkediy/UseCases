package com.redislabs;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class TimeSeries {
	Jedis jedis = null;
	static public final String [] types = {"Login", "Logout", "New", "Update", "Delete", "Replace" };
	
	public TimeSeries(String host, int port) {
		jedis = new Jedis(host, port);
	}

	public void addEvent(Event e)
	{
		Pipeline p = jedis.pipelined();
		p.multi();
		p.hmset("" + e.id, e.toMap());
		p.zadd("events", e.timestamp, "" + e.id);
		p.exec();
		jedis.resetState();
	}
	
	public int countEventsForType(String type, long time)
	{
		long now = System.currentTimeMillis();
		long startTime = now - (time * 1000);
		
		int n = 0;
		Set<String> ids = jedis.zrangeByScore("events", startTime, now);
		for (String id: ids)
		{
			String eType = jedis.hget(id, "type");
			if ( eType.equals(type) )
				n++;
		}
		return n;
	}
	
	public class Event
	{
		int id;
		long timestamp;
		String type;
		String param;
		
		
		Event(int id, long timestamp, String type, String param)
		{
			this.id = id;
			this.timestamp = timestamp;
			this.type = type;
			this.param = param;
		}
		
		Map<String, String> toMap()
		{
			Map<String, String> m = new HashMap<String, String>();
			m.put("id", "" + id);
			m.put("timestamp", "" + timestamp);
			m.put("type", type);
			m.put("param", param);
			
			return m;
		}
	}
	
	public static void main(String[] args) {
		TimeSeries ts = new TimeSeries("localhost", 6379);
		
		Random rand = new Random();
		long now = System.currentTimeMillis();
		for ( int i = 0; i < 10000; i++ )
		{
			ts.addEvent(ts.new Event(i, now - (rand.nextLong() % 86400000), types[rand.nextInt(types.length)], "asdf"));
		}

		for ( String t: types)
		{
			System.out.println(t + ": " + ts.countEventsForType(t, 600));
		}
	}

}
