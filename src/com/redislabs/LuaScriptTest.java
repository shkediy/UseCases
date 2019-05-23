package com.redislabs;

import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import redis.clients.jedis.Jedis;

public class LuaScriptTest {
	Jedis jedis = null;
	final String script = "local tmp = redis.call('get', KEYS[1]);"
			+ "tmp = string.gsub(tmp, '(\"timestamp\":)(%d+)', function(a,b) return a..ARGV[1] end)" 
			+ "redis.call('set', KEYS[1], tmp)"
			+ "return {redis.call('get', KEYS[1])}";
	String sha;
	 
	public LuaScriptTest(String host, int port) {
		jedis = new Jedis(host, port);
		sha = jedis.scriptLoad(script);
		GsonBuilder builder = new GsonBuilder();
	    Gson gson = builder.create();
	   
		String json = gson.toJson(new Test());
		jedis.set("test", json);
	}

	public class Test {
		int num = 10;
		long timestamp = System.currentTimeMillis();
		String str = "XXXXX";
	}
	
	
	public List<String> exec(String key, String arg)
	{
		return (List<String>) jedis.evalsha(sha, 1, key, arg);
	}
	
	public static void main(String[] args) {
		
		LuaScriptTest test = new LuaScriptTest("localhost", 6379);
		System.out.println(test.exec("test", Long.toString(System.currentTimeMillis())));
		System.out.println(test.exec("test", Long.toString(System.currentTimeMillis())));
	}
}
