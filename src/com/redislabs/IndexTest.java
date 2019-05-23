package com.redislabs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class IndexTest {

	private Jedis jedis = null;
	private AtomicInteger userId = new AtomicInteger();
	
	public IndexTest(String host, int port) {
		jedis = new Jedis(host, port);
	}

	public class User {
		String fname;
		String lname;
		String state;
		
		public User(String fname, String lname, String state)
		{
			this.fname = fname;
			this.lname = lname;
			this.state = state;
		}
		
		public Map<String, String> toMap()
		{
			Map<String, String> m = new HashMap<String, String>();
			m.put("fname", fname);
			m.put("lname", lname);
			m.put("state", state);
			return m;
		}
		
	}
	
	public void addUser(User user)
	{
		String key = "user:" + userId.incrementAndGet();
		Pipeline p = jedis.pipelined();
		p.multi();
		p.hmset(key, user.toMap());
		p.sadd("users", key);
		p.exec();
		jedis.resetState();
	}
	
	public void createIndex(String parameter)
	{
		Set<String> keys = jedis.smembers("users");
		for ( String k : keys)
		{
			String val = jedis.hget(k, parameter);
			jedis.sadd("users:" + parameter + ":" + val, k);
		}
	}
	
	public List<String> getAllUsers()
	{
		return new ArrayList<String>(jedis.smembers("users"));
	}
	
	public List<String> getUserByParameter(String parameter, String value)
	{
		
		if ( jedis.exists("users:" + parameter + ":" + value))
		{
			Set<String> keys = jedis.smembers("users:" + parameter + ":" + value);
			return new ArrayList<String>(keys);
		}
		else
		{
			List<String> users = new ArrayList<String>();
			Set<String> keys = jedis.smembers("users");
			for ( String k : keys)
				if ( jedis.hget(k, parameter).equals(value) )
					users.add(k);
			return users;
		}
	}
	
	public List<String> getValue(List<String> users, String parameter)
	{
		List<String> values = new ArrayList<String>();
		for ( String user : users )
		{
			values.add(jedis.hget(user, parameter));
		}
		
		return values;
	}
	
	public static void main(String[] args) {
		IndexTest test = new IndexTest("localhost", 6379);
		test.addUser(test.new User("John", "Cage", "CA"));
		test.addUser(test.new User("Jeff", "Rastin", "LA"));
		test.addUser(test.new User("Jane", "White", "NY"));
		test.addUser(test.new User("Jack", "Barnes", "WA"));
		test.addUser(test.new User("Jill", "Carry", "CA"));
		test.addUser(test.new User("Rachel", "Millwall", "NY"));
		test.addUser(test.new User("Sarah", "Page", "TX"));
		test.addUser(test.new User("Steve", "Stone", "CA"));
		test.addUser(test.new User("John", "Rubin", "NY"));
		test.addUser(test.new User("George", "Blake", "NC"));
		test.addUser(test.new User("Fran", "Stout", "CA"));
		test.addUser(test.new User("Lily", "Marks", "NY"));
		
		test.createIndex("state");
		
		System.out.println("First name of all users: " + test.getValue(test.getAllUsers(), "fname"));
		System.out.println("First name of user with last name Carry: " + test.getValue(test.getUserByParameter("lname", "Carry"), "fname"));
		System.out.println("Last name of users from CA: " + test.getValue(test.getUserByParameter("state", "CA"), "lname"));
		
	}

}
