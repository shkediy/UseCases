package com.redislabs;

import java.util.List;

import redis.clients.jedis.Jedis;

public class GeoSpatialTest {
	private Jedis jedis = null;
	private String key = "myGeo";
	
	// Lua script to compute the total distance between point 1 and 3
	final String script = "local dist1 = redis.call('geodist', KEYS[1], ARGV[1], ARGV[2]);"
			+ "local dist2 = redis.call('geodist', KEYS[1], ARGV[2], ARGV[3]);"
			+ "return {dist1 + dist2}";
	private String sha;
	
	public GeoSpatialTest() {
		jedis = new Jedis("localhost", 6379);
		sha = jedis.scriptLoad(script);
	}

	//add a geo location
	public void add(String name, double longitude, double latitude) {
		jedis.geoadd(key, longitude, latitude, name);
	}
	
	//Get the total distance between 3 locations using 2 separate calls.
	public long dist3(String first, String second, String third) {
		return (long) (jedis.geodist(key, first, second) + jedis.geodist(key, second, third));
	}
	
	//Get the total distance between 3 locations using lua script.
	public long dist3Atomic(String first, String second, String third) {
		List<Long> res = (List<Long>) jedis.evalsha(sha, 1, key, first, second, third);
		return res.get(0);
	}
	
	public static void main(String[] args) {
		GeoSpatialTest test = new GeoSpatialTest();
		test.add("London", -0.127758, 51.507351);
		test.add("Paris", 2.352222, 48.856614);
		test.add("Berlin", 13.404954, 52.520007);
	
		long start = System.currentTimeMillis();
		System.out.println("Distance: " + test.dist3("London", "Paris", "Berlin"));
		System.out.println(System.currentTimeMillis() - start);
		start = System.currentTimeMillis();
		System.out.println("Distance: " + test.dist3Atomic("London", "Paris", "Berlin"));
		System.out.println(System.currentTimeMillis() - start);
	}

}
