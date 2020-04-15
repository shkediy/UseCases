package com.redislabs;

import java.text.SimpleDateFormat;
import java.util.Date;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class ProducerConsumerList {
	
	private JedisPool pool;
	private final String listName = "messages";
	
	public ProducerConsumerList(String host, int port) {
		pool = new JedisPool(host, port);
	}

	public void sendMessgae() {
		try ( Jedis jedis =  pool.getResource() ) {
			String message = "Message set at " +  new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss.SSS").format(new Date());
			jedis.lpush(listName, message);
		}
	}
	
	public class Consumer implements Runnable {
		String procListName;
		private boolean running = true;
		
		public Consumer(String name)
		{
			this.procListName = "proc:subs:" + name;
		}
		
		public void stop() {
			running = false;
		}
		
		private void getMessage() {
			try ( Jedis jedis = pool.getResource() ) {
				String resp = jedis.brpoplpush(listName, procListName, 1);
				if ( resp != null )
				{
					System.out.println("Message received: " + procListName + ": " + resp);
					jedis.lpop(procListName);
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
		ProducerConsumerList test = new ProducerConsumerList("localhost", 6379);
		
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
