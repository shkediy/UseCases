package com.redislabs;

import java.util.Random;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;

import redis.clients.jedis.Jedis;

public class RedisBitmaps {
	
	static private final int cPeriods = 3;
	static private final long cPeriodTime = 300;
	static private final String cKey = "devices";
	static public final long cMaxDeviceId = 1000;
	
	Jedis jedis = null;
	Timer timer = new Timer();
	Thread updater = new BitmapsUpdater();
	
	public RedisBitmaps(String host, int port) {
		jedis = new Jedis(host, port);
		timer.scheduleAtFixedRate(new PeriodTask(), cPeriodTime  * 1000, cPeriodTime * 1000);
		updater.start();
	}

	public synchronized void touchDevice(long deviceId)
	{
		jedis.setbit(cKey, deviceId, true);
	}
	
	private synchronized void advancePeriods()
	{
		String val = jedis.getSet(cKey, "");
		for (int i = 1; i < cPeriods; i++)
		{
			if ( val == null )
				break;
			val = jedis.getSet(cKey + "-" + i, val);
		}
	}
	
	public synchronized boolean  isDeviceSet(long deviceId)
	{
		if ( jedis.getbit(cKey, deviceId) )
			return true;
		
		for (int i = 1; i < cPeriods - 1; i++)
		{
			if ( jedis.getbit(cKey + "-" + i, deviceId) )
				return true;
		}
		
		return false;
	}
	
	public class PeriodTask extends TimerTask
	{
		public void run() {
			System.out.println("Starting period advancing...");
			advancePeriods();
			System.out.println("Finished period advancing...");
		}
	}
	
	public class BitmapsUpdater extends Thread
	{
		public void run() {
			Random rand = new Random();
			while ( true )
			{
				long deviceId = Math.abs(rand.nextLong()) % cMaxDeviceId;
				System.out.println("Touching device " + deviceId);
				touchDevice(deviceId);
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					break;
				}
			}
		}
	}
	
	public void stop() throws InterruptedException
	{
		timer.cancel();
		updater.interrupt();
		updater.join();
	}
	
	public static void main(String[] args) {
		RedisBitmaps bm = new RedisBitmaps("localhost", 6379);
		try (Scanner scan = new Scanner(System.in)) {
			while ( true )
			{
				System.out.print("Enter device id:");
				if ( !scan.hasNextLong() )
					break;
				long id = scan.nextLong();
				System.out.println("Device " + id + " is" + ((bm.isDeviceSet(id)) ? "" : " NOT") + " active.");
				
			}
		}
		try {
			bm.stop();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Stopped!");
	}

}
