package com.flatironschool.javacs;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

import redis.clients.jedis.Transaction;

public class SQLDatabase {
	
	static String dbDriver = "org.postgresql.Driver";
	static String dbServer = "jdbc:postgresql://127.0.0.1:5432/";
	static String dbUser   = "postgres";
	static String dbPass   = "database";
	static String dbName   = "postgres";
	
	Connection connection;
	
	public SQLDatabase() throws SQLException, ClassNotFoundException {
		Class.forName(dbDriver);
	    connection = DriverManager.getConnection(dbServer + dbName, dbUser, dbPass);
	}
	
	public void close() throws SQLException {
		connection.close();
	}
	
	public long sadd(String key, String... members) throws SQLException, ClassNotFoundException {
		//return 1 if value is added return 0 if it already exists
		String querySet = "insert into sets values (\'" + key + "\')";
		
		PreparedStatement pstmt = connection.prepareStatement(querySet);
      try {
		   pstmt.executeQuery();
        }
        catch (Exception e){
        }
		
		for(String member: members) {
         String querySetValues = "insert into setValues values (\'" + key + "\', \'" + member + "\')";
			PreparedStatement pstmt2 = connection.prepareStatement(querySetValues);
         try {
			   pstmt2.executeQuery();
         }
         catch (Exception e) {
         
         } 
		}
		
		return 0;
	}
	
	public boolean sismember(String set, String element) throws SQLException, ClassNotFoundException{
		String query = "select value from setValues where setID = \'" + set + "\'";
		PreparedStatement pstmt = connection.prepareStatement(query);
		ResultSet result = pstmt.executeQuery();
		
		while(result.next()) {
			if(result.getString(1).toLowerCase().equals(element.toLowerCase())) {
				return true;
			}
		}
		
		return false;
	}
	
	public Set<String> smembers(String set) throws SQLException, ClassNotFoundException{
		Set<String> done = new HashSet<String>();
		
		String query = "select value from setValues where setID = \'" + set + "\'";
		PreparedStatement pstmt = connection.prepareStatement(query);
		ResultSet result = pstmt.executeQuery();
		
		while(result.next()) {
			done.add(result.getString(1).toLowerCase());
		}
		
		return done;
	}
	
	public long hset(String hash, String field, String value) throws SQLException, ClassNotFoundException{
		//if field already exist, return 0
		//otherwise return 1
		
		String queryHash = "insert into hash values (\'" + hash.toLowerCase() + "\')";
		String HashField = "insert into hashField values (\'" + hash.toLowerCase() + "\', \'" + field +"\')";
		String HashValues = "insert into hashValues values (\'" + hash.toLowerCase() + "\', \'" + field + "\', \'" + value +"\')";
				
		PreparedStatement pstmt = connection.prepareStatement(queryHash);
      try {
		   pstmt.executeQuery();
      } catch (Exception e) {
      
      }

		PreparedStatement pstmt2 = connection.prepareStatement(HashField);
      try {
		 pstmt2.executeQuery();	
		} catch (Exception e) {
      
      }
		PreparedStatement pstmt3 = connection.prepareStatement(HashValues);
      
      try {
		   pstmt3.executeQuery();
      } catch (Exception e) {
      
      }
					
		return 0;
	}
	
	public String hget(String hash, String field) throws SQLException, ClassNotFoundException{
		String HashValues = "select value from hashValues where hashID = \'" + hash.toLowerCase() + "\' AND fieldID = \'" + field.toLowerCase() +"\'";
		
		PreparedStatement pstmt = connection.prepareStatement(HashValues);
      
		ResultSet result = pstmt.executeQuery();
		
		String give = "";
		while (result.next()) {
			give = result.getString(1);
		}
		
		return give;
	}

	public long hincrBy(String key, String field, long value) throws SQLException, ClassNotFoundException {
		String HashValues = "select value from hashValues where hashID = \'" + key + "\' AND fieldID = \'" + field +"\'";
		
		PreparedStatement pstmt = connection.prepareStatement(HashValues);
      
		ResultSet result = pstmt.executeQuery();
      
		if(result.next() == false) {
         hset(key, field, "1");
         return 1;
      }; 
      
		long sum = Long.parseLong(result.getString(1)) + value;
		//returns new value at specified field
      
      HashValues = "update hashValues set value = " + sum + " where hashID = \'" + key + "\' AND fieldID = \'" + field +"\'";
      pstmt = connection.prepareStatement(HashValues);
      try {
		   result = pstmt.executeQuery();
      } catch (Exception e) {
      }
      
		return sum;
	}
	
	public void deleteURLSets() throws SQLException, ClassNotFoundException {
		String sets = "delete from sets cascade";
		
		PreparedStatement pstmt = connection.prepareStatement(sets);
		
		try {
			pstmt.executeQuery();
		} catch (Exception e) {
			
		}
	}
	
	public void deleteTermCounters() throws SQLException, ClassNotFoundException {
		String hash = "delete from hash cascade";
		
		PreparedStatement pstmt = connection.prepareStatement(hash);
		
		try {
			pstmt.executeQuery();
		} catch (Exception e) {
			
		}
	}
	
	public void deleteAllKeys() throws SQLException, ClassNotFoundException {
		String all = "delete from sets cascade; delete from hash cascade";
		
		PreparedStatement pstmt = connection.prepareStatement(all);
		
		try {
			pstmt.executeQuery();
		} catch (Exception e) {
			
		}
	}
	
	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		
		SQLDatabase test = new SQLDatabase();
      
      // Set
	    test.sadd("myset", "element1", "element2", "element3");
	    System.out.println("element2 is member: " + test.sismember("myset", "element2"));

	    // Hash
	    test.hset("myhash", "word1", Integer.toString(2));
	    test.hincrBy("myhash", "word2", 1);
	    System.out.println("frequency of word1: " + test.hget("myhash", "word1"));
	    System.out.println("frequency of word2: " + test.hget("myhash", "word2"));
       
       test.close();
	}
}