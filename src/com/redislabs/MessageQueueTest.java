package com.redislabs;
import java.util.List;
import java.util.Scanner;

import redis.clients.jedis.Jedis;

public class MessageQueueTest {
	Jedis jedis = null;
	
	public MessageQueueTest(String host, int port) {
			jedis = new Jedis(host, port);
	}

	public void writeMessage(String message)
	{
		jedis.lpush("queue", message);
	}
	
	
	
	public class MessageReceiver extends Thread {
		private Jedis conn;
		private boolean stop = false;
		public MessageReceiver(Jedis conn)
		{
			this.conn = conn;
		}
		
		private String readMessage(int timeout)
		{
			List<String> res = conn.brpop(timeout, "queue");
			if ( res != null && res.size() == 2 )
				return res.get(1);
			
			return null;
		}
		
		public void quit()
		{
			stop = true;
		}
		
		public void run()
		{
			while ( !stop )
			{
				try
				{
					String m = readMessage(10);
					if (m != null)
					{
						System.out.println("Received message: " + m);
					}
					Thread.sleep(100);
				}
				catch (InterruptedException e)
				{
					stop = true;
				}
			}
		}
	}
	
	public static void main(String[] args) {
		MessageQueueTest test = new MessageQueueTest("localhost", 6379);
		
		MessageReceiver mr = test.new MessageReceiver(new Jedis("localhost", 6379));
		mr.start();
		try (Scanner scan = new Scanner(System.in)) {
			while ( true )
			{
				System.out.print("Enter message to send:");
				if ( !scan.hasNext() )
					break;
				String message = scan.next();
				if (message.equalsIgnoreCase("quit") || message.equalsIgnoreCase("exit"))
					break;
				test.writeMessage(message);
			}
		}
		mr.quit();

	}

}
