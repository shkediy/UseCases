package com.redislabs;

import java.text.SimpleDateFormat;
import java.util.AbstractMap;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;

public class ProducerConsumerStreams {
	
	private JedisPool pool;
	private final String streamName = "mystream";
	private final String groupName = "mygroup";
	public ProducerConsumerStreams(String host, int port) {
		pool = new JedisPool(host, port);
		try ( Jedis jedis =  pool.getResource() ) {
			if ( jedis.exists(streamName) ) 
				jedis.xgroupDestroy(streamName, groupName);
			jedis.xgroupCreate(streamName, groupName, StreamEntryID.LAST_ENTRY, true);
		}
	}

	public void sendMessgae() {
		try ( Jedis jedis =  pool.getResource() ) {
			String time = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss.SSS").format(new Date());
			Map<String, String> message = new HashMap<String, String>();
			message.put("time", time);
			jedis.xadd(streamName, StreamEntryID.NEW_ENTRY, message);
		}
	}
	
	public class Consumer implements Runnable {
		private boolean running = true;
		private String name;
		public Consumer(String name)
		{
			this.name = name;
		}
		
		public void stop() {
			running = false;
		}
		
		private void getMessage() {
			try ( Jedis jedis = pool.getResource() ) {
				List<Entry<String, List<StreamEntry>>> resp = jedis.xreadGroup(groupName, "cons:" + name, 1, 1000, false, new AbstractMap.SimpleEntry<String, StreamEntryID>(streamName, StreamEntryID.UNRECEIVED_ENTRY));
				if ( resp != null ) {
					for ( Entry<String, List<StreamEntry>> r : resp ) {
						System.out.println("Consumer: " + name + " --> " + r.getKey() + " -- " + r.getValue().toString());
						for ( StreamEntry e : r.getValue() ) {
							jedis.xack(streamName, groupName, e.getID());
						}
					}
				}
			}
		}
		
		@Override
		public void run() {
			while ( running )
				getMessage();
		}
		
	}
	
	public static void main(String[] args) throws InterruptedException {
		ProducerConsumerStreams test = new ProducerConsumerStreams("localhost", 6379);
		
		Consumer cons1 = test.new Consumer("1");
		Consumer cons2 = test.new Consumer("2");
		new Thread(cons1).start();
		new Thread(cons2).start();
		
		int n = 10;
		int delay = 500;
		while ( n-- > 0 ) {
			test.sendMessgae();
			Thread.sleep(delay);
		}
		
		cons1.stop();
		cons2.stop();
		
	}
}
