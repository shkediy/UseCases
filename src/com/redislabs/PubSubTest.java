package com.redislabs;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class PubSubTest {
	Jedis jedis = null;
	
	public PubSubTest(String host, int port) {
		jedis = new Jedis(host, port);	
	}

	public void publish(String channel, String message)
	{
		jedis.publish(channel, message);
	}
	
	public class Subscriber extends JedisPubSub implements Runnable {
		private String [] channels;
		Jedis jedis;
		public Subscriber(String [] channels)
		{
			this.channels = channels;
			jedis = new Jedis("localhost", 6379);
		}
		
		@Override
		public void onMessage(String channel, String message) {
			System.out.println(Thread.currentThread().getName() + " -- " + channel + ": " + message);
		}

		@Override
		public void run() {
			jedis.subscribe(this,channels);
		}
		
	}
	
	public static void main(String[] args) {
		PubSubTest test = new PubSubTest("localhost", 6379);
		
		final String [] channelNames = {"c1", "c2", "c3"};
		
		Subscriber subscriber1 = test.new Subscriber(channelNames);
		new Thread(subscriber1).start();
		
		Subscriber subscriber2 = test.new Subscriber(channelNames);
		new Thread(subscriber2).start();
		
		for (int i = 0; i < 10; i++)
		{
			test.publish(channelNames[i % channelNames.length], "xxxxxxxxxxxxxxxxxxxx");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		subscriber1.unsubscribe(channelNames);
		subscriber2.unsubscribe(channelNames);
	}

}
