package com.redislabs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.GeoRadiusParam;

public class SimpleTest {

	public static void main(String[] args) {
		
		
		Jedis j = new Jedis("localhost", 6379);
		j.set("key1", "value1");
		System.out.println(j.get("key1"));
		j.del("key1");
		j.close();
		
		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
		jedisPoolConfig.setMaxWaitMillis(10000);
		jedisPoolConfig.setMaxTotal(128);
	    	
		JedisPool pool = new JedisPool(jedisPoolConfig, "localhost", 6379, 10000);
		
		try ( Jedis jedis = pool.getResource() ) {
			jedis.set("key1", "value1");
			System.out.println(jedis.get("key1"));
			jedis.del("key1");
			System.out.println("");
		
			
			jedis.set("userid:1", "8754");
			System.out.println("User ID: " + jedis.get("userid:1"));
			jedis.expire("userid:1", 60);
			jedis.del("userid:1");
			System.out.println("");
			
			Map<String, String> session = new HashMap<String, String>();
			session.put("userid", "8754");
			session.put("name", "dave");
			session.put("ip", "10:20:104:31");
			session.put("hits", "1");
			
			jedis.hmset("usersession:1", session);
			System.out.println("Seesion parameters: " + jedis.hmget("usersession:1", "userid", "name","ip" ,"hits"));
			jedis.hincrBy("usersession:1", "hits", 1);
			System.out.println("Seesion parameters: " + jedis.hgetAll("usersession:1"));
			jedis.hset("usersession:1", "lastpage", "home");
			System.out.println("Last page: " + jedis.hget("usersession:1", "lastpage"));
			System.out.println("Seesion parameters: " + jedis.hgetAll("usersession:1"));
			jedis.hdel("usersession:1", "lastpage");
			System.out.println("");
			
			jedis.lpush("queue1", "orange");
			jedis.lpush("queue1", "green");
			jedis.lpush("queue1", "blue");
			jedis.rpush("queue1", "red");
			jedis.rpoplpush("queue1", "queue2");
			System.out.println("Queue1: " + jedis.lrange("queue1", 0, -1));
			System.out.println("Queue2: " + jedis.lrange("queue2", 0, -1));
			System.out.println("");
			
			jedis.sadd("science", "article:3" ,"article:1");
			jedis.sadd("tech", "article:22","article:14", "article:3");
			jedis.sadd("education", "article:9", "article:3", "article:2");
			
			System.out.println("Articles with science: " + jedis.smembers("science"));
			System.out.println("Interstion of all: " + jedis.sinter("science", "tech", "education"));
			System.out.println("");
			
			jedis.zadd("game:1", 10000, "id:1" );
			jedis.zadd("game:1", 21000, "id:2");
			jedis.zadd("game:1", 34000, "id:3" );
			jedis.zadd("game:1", 35000, "id:4");

			jedis.zincrby("game:1", 10000,"id:3");

			System.out.println("Game leader: " + jedis.zrevrange("game:1", 0,0));
			System.out.println("First 2 places with scores: " + jedis.zrevrangeWithScores("game:1", 0,1));
			System.out.println("");
			
			jedis.geoadd("pharmacies", -0.310392, 51.456454, "Charles Harry Pharmacy");
			jedis.geoadd("pharmacies",-0.296402, 51.462069, "Richmond Pharmacy");
			jedis.geoadd("pharmacies", -0.318604, 51.455338, "St Margerets Pharmacy");
			
			List<GeoRadiusResponse> res = jedis.georadius("pharmacies", -0.30566239999996014, 51.452921, 1000 , 
					GeoUnit.M, GeoRadiusParam.geoRadiusParam().withCoord().withCoord().withDist());
			for ( GeoRadiusResponse r: res )
				System.out.println(r.getMemberByString() + " : " + r.getDistance() + " : " + r.getCoordinate());
			System.out.println("");
			
			jedis.pfadd("visitors:20160921", "86.163.34.208");
			jedis.pfadd("visitors:20160921", "52.203.210.236");
			jedis.pfadd("visitors:20160921", "54.87.203.132");
			jedis.pfadd("visitors:20160921", "54.87.201.121");
			jedis.pfadd("visitors:20160921", "52.203.210.236");
			System.out.println("Visitors: " + jedis.pfcount("visitors:20160921"));


		}

		pool.close();	
		
	}

}
