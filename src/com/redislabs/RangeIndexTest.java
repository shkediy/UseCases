package com.redislabs;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.math.RandomUtils;

import redis.clients.jedis.Jedis;

public class RangeIndexTest {
	private Jedis jedis = null;
	
	static final String alphaNumeric = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	static final String numeric = "0123456789";
	
	public RangeIndexTest(String host, int port) {
		jedis = new Jedis(host, port);
	}
	
	public void addCar(String vin, long makeYear)
	{
		jedis.sadd("cars:" + makeYear, vin);
		jedis.zadd("cars", makeYear, "cars:" + makeYear);
	}
	
	public String generateVin()
	{
		StringBuilder sb = new StringBuilder(17);
		int pos = RandomUtils.nextInt(numeric.length());
		sb.append(RandomUtils.nextInt(10));
		for( int i = 0; i < 10; i++ )
		{
			pos = RandomUtils.nextInt(alphaNumeric.length());
			sb.append(alphaNumeric.charAt(pos));
		}
		sb.append(100000 + RandomUtils.nextInt(100000));
		return sb.toString();
	}
	
	public List<String> getCarsForYear(int year)
	{
		return new ArrayList<String>(jedis.smembers("cars:" + year));
	}
	
	public List<String> getCarsForYearRange(int startYear, int endYear)
	{
		List<String> res = new ArrayList<String>();
		Set<String> years = jedis.zrangeByScore("cars", startYear, endYear);
		for ( String y : years)
		{
			res.addAll(jedis.smembers(y));
		}
		
		return res;
	}
	
	public static void main(String[] args) {
		RangeIndexTest test = new RangeIndexTest("localhost", 6379);
		int currentYear = Calendar.getInstance().get(Calendar.YEAR);
		for ( int i = 0; i < 1000; i++ )
		{
			test.addCar(test.generateVin(), currentYear - RandomUtils.nextInt(30));
		}
		
		System.out.println("Cars of 2013: " + test.getCarsForYear(2013));
		System.out.println("Cars before 1990: " + test.getCarsForYearRange(0, 1990));
		System.out.println("Cars between 2017 and 2019: " + test.getCarsForYearRange(2017, 2020));
	}

}
