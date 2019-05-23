package com.redislabs;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class PubSubList {
	
	private JedisPool pool;
	public PubSubList(String host, int port) {
		pool = new JedisPool(host, port);
	}

	public void sendMessgae() {
		try ( Jedis jedis =  pool.getResource() ) {
			Set<String> subs = jedis.smembers("subscribers");
			String message = "Message set at " +  new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss.SSS").format(new Date());
			for ( String s : subs ) {
				jedis.lpush(s, message);
			}
		}
	}
	
	public class Consumer implements Runnable {
		String listName;
		private boolean running = true;
		
		public Consumer(String name)
		{
			this.listName = "subs:" + name;
			try ( Jedis jedis = pool.getResource() ) {
				jedis.sadd("subscribers", listName);
			}
		}
		
		public void stop() {
			running = false;
		}
		
		private void getMessage() {
			try ( Jedis jedis = pool.getResource() ) {
				List<String> resp = jedis.brpop(1, listName);
				if ( resp != null )
				{
					System.out.println("Message received: " + resp.get(0) + ": " + resp.get(1));
				}
				else
					System.out.println("No message found");
			}
		}
		
		@Override
		public void run() {
			while ( running )
				getMessage();
		}
		
	}
	
	public static void main(String[] args) throws InterruptedException {
		PubSubList test = new PubSubList("localhost", 6379);
		
		Consumer cons1 = test.new Consumer("1");
		new Thread(cons1).start();
		Consumer cons2 = test.new Consumer("2");
		new Thread(cons2).start();
		
		int n = 10;
		int delay = 1000;
		while ( n-- > 0 ) {
			test.sendMessgae();
			Thread.sleep(delay);
		}
		
		cons1.stop();
		cons2.stop();
		
	}
}
