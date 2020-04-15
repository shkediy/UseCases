package com.redislabs;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.math.RandomUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class CarInventorySolution {
	private JedisPool pool;
	int currentYear = Calendar.getInstance().get(Calendar.YEAR);
	
	static final String alphaNumeric = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	static final String numeric = "0123456789";
	static final String[] states = { 
		  "AK","AL","AR","AS","AZ","CA","CO","CT","DC","DE","FL",
		  "GA","GU","HI","IA","ID","IL","IN","KS","KY","LA","MA","MD",
          "ME","MI","MN","MO","MS","MT","NC","ND","NE","NH","NJ","NM",
          "NV","NY","OH","OK","OR","PA","PR","RI","SC","SD","TN","TX",
          "UT","VA","VI","VT","WA","WI","WV","WY" 
    };
	static final String[] brands = { 
			"Alfa Romeo","Audi","Bentley","BMW","Bugatti",
			"Cadillac","Chevrolet","Chrysler","Corvette","Dodge","Ferrari",
			"Fiat","Ford","Honda","Hummer","Hyundai","Jaguar","Jeep","KIA",
			"Lamborghini","Land Rover","Lexus","Lincoln",
			"Maserati","Mazda","McLaren","Mercedes-Benz",
			"Mini","Mitsubishi","Nissan","Porsche",
			"Rover","Saab","Smart","Subaru","Toyota","Volkswagen","Volvo"
	};
	
	public CarInventorySolution(String host, int port) {
		pool = new JedisPool(host, port);
	}
	
	public void addCar()
	{
		Map<String, String> m = new HashMap<String, String>();
		m.put("year", "" + (currentYear - RandomUtils.nextInt(30)));
		m.put("brand", brands[RandomUtils.nextInt(brands.length)]);
		m.put("state", states[RandomUtils.nextInt(states.length)]);
		String vin = generateVin();
		try ( Jedis jedis =  pool.getResource() ) {	
			jedis.hmset(vin, m);
			jedis.sadd("cars:all", vin);
			jedis.sadd("cars:brand:" + m.get("brand"), vin);
			jedis.sadd("cars:state:" + m.get("state"), vin);
			jedis.sadd("cars:year:" + m.get("year"), vin);
			jedis.zincrby("cars:brands", 1, m.get("brand"));
		}
	}
	
	private String generateVin()
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
	
	public long getNumberOfCars()
	{
		try ( Jedis jedis =  pool.getResource() ) {
			return jedis.scard("cars:all");
		}
	}
	
	public Set<String> getVinsForYear(int year)
	{
		try ( Jedis jedis =  pool.getResource() ) {
			return jedis.smembers("cars:year:" + year);
		}
	}
	
	public Set<String> getVinsForBrandAndState(String brand, String state) {
		try ( Jedis jedis =  pool.getResource() ) {
			return jedis.sinter("cars:brand:" + brand, "cars:state:" + state);
		}
	}
	
	public Set<String> getTopBrands(int num)
	{
		try ( Jedis jedis =  pool.getResource() ) {
			return jedis.zrevrange("cars:brands", 0, num - 1);
		}
	}
	
	public static void main(String[] args) {
		CarInventorySolution test = new CarInventorySolution(args[0], Integer.parseInt(args[1]));
		for ( int i = 0; i < 1000; i++ )
		{
			test.addCar();
		}
		
		System.out.println("Total cars: " + test.getNumberOfCars());
		System.out.println("Cars of 2013: " + test.getVinsForYear(2013));
		System.out.println("Ford cars in Arizona: " + test.getVinsForBrandAndState("Ford", "AZ"));
		System.out.println("Top 5 brands: " + test.getTopBrands(5));
	}

}
