package com.redislabs;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.math.RandomUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class CarInventory {
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
	
	public CarInventory(String host, int port) {
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
		//TODO: Add missing code
		return 0;
	}
	
	public Set<String> getVinsForYear(int year)
	{
		//TODO: Add missing code
		return null;
	}
	
	public Set<String> getVinsForBrandAndState(String brand, String state) {
		//TODO: Add missing code
		return null;
	}
	
	public Set<String> getTopBrands(int num)
	{
		//TODO: Add missing code
		return null;
	}
	
	public static void main(String[] args) {
		CarInventory test = new CarInventory(args[0], Integer.parseInt(args[1]));
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
