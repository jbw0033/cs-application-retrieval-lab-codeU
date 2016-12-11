package com.flatironschool.javacs;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.jsoup.select.Elements;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

/**
 * Represents a Redis-backed web search index.
 * 
 */
public class JedisIndex {

	private SQLDatabase jedis;

	/**
	 * Constructor.
	 * 
	 * @param jedis
	 */
	public JedisIndex(SQLDatabase jedis) {
		this.jedis = jedis;
	}
	
	/**
	 * Returns the Redis key for a given search term.
	 * 
	 * @return Redis key.
	 */
	private String urlSetKey(String term) {
		return "URLSet:" + term;
	}
	
	/**
	 * Returns the Redis key for a URL's TermCounter.
	 * 
	 * @return Redis key.
	 */
	private String termCounterKey(String url) {
		return "TermCounter:" + url;
	}

	/**
	 * Checks whether we have a TermCounter for a given URL.
	 * 
	 * @param url
	 * @return
	 */
//	public boolean isIndexed(String url) {
//		String redisKey = termCounterKey(url);
//		return jedis.exists(redisKey);
//	}
	
	/**
	 * Looks up a search term and returns a set of URLs.
	 * 
	 * @param term
	 * @return Set of URLs.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public Set<String> getURLs(String term) throws ClassNotFoundException, SQLException {
		Set<String> result = jedis.smembers(urlSetKey(term));
		return result;
	}

    /**
	 * Looks up a term and returns a map from URL to count.
	 * 
	 * @param term
	 * @return Map from URL to count.
     * @throws SQLException 
     * @throws ClassNotFoundException 
	 */
	public Map<String, Integer> getCounts(String term) throws ClassNotFoundException, SQLException {
		Map<String, Integer> result = new HashMap<String, Integer>();
        Set<String> helper = getURLs(term);
        for(String url: helper) {
        	result.put(url, getCount(url, term));
        }
		return result;
	}

    /**
	 * Returns the number of times the given term appears at the given URL.
	 * 
	 * @param url
	 * @param term
	 * @return
     * @throws SQLException 
     * @throws ClassNotFoundException 
     * @throws NumberFormatException 
	 */
	public Integer getCount(String url, String term) throws ClassNotFoundException, SQLException {
//		System.out.println(termCounterKey(url));
//		System.out.println(term);
//		System.out.println(jedis.hget(termCounterKey(url), term));
		return Integer.parseInt(jedis.hget(termCounterKey(url), term));
	}


	/**
	 * Add a page to the index.
	 * 
	 * @param url         URL of the page.
	 * @param paragraphs  Collection of elements that should be indexed.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public void indexPage(String url, Elements paragraphs) throws ClassNotFoundException, SQLException {
		TermCounter tc = new TermCounter(url);
		tc.processElements(paragraphs);
		
		for(String term: tc.keySet()) {
			jedis.sadd(urlSetKey(term).toLowerCase(), url.toLowerCase());
			jedis.hset(termCounterKey(url).toLowerCase(), term.toLowerCase(), tc.get(term).toString());
		}
	}

	/**
	 * Prints the contents of the index.
	 * 
	 * Should be used for development and testing, not production.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
//	public void printIndex() throws ClassNotFoundException, SQLException {
//		// loop through the search terms
//		for (String term: termSet()) {
//			System.out.println(term);
//			
//			// for each term, print the pages where it appears
//			Set<String> urls = getURLs(term);
//			for (String url: urls) {
//				Integer count = getCount(url, term);
//				System.out.println("    " + url + " " + count);
//			}
//		}
//	}

	/**
	 * Returns the set of terms that have been indexed.
	 * 
	 * Should be used for development and testing, not production.
	 * 
	 * @return
	 */
//	public Set<String> termSet() {
//		Set<String> keys = urlSetKeys();
//		Set<String> terms = new HashSet<String>();
//		for (String key: keys) {
//			String[] array = key.split(":");
//			if (array.length < 2) {
//				terms.add("");
//			} else {
//				terms.add(array[1]);
//			}
//		}
//		return terms;
//	}

	/**
	 * Returns URLSet keys for the terms that have been indexed.
	 * 
	 * Should be used for development and testing, not production.
	 * 
	 * @return
	 */
//	public Set<String> urlSetKeys() {
//		return jedis.keys("URLSet:*");
//	}

	/**
	 * Returns TermCounter keys for the URLS that have been indexed.
	 * 
	 * Should be used for development and testing, not production.
	 * 
	 * @return
	 */
//	public Set<String> termCounterKeys() {
//		return jedis.keys("TermCounter:*");
//	}

	/**
	 * Deletes all URLSet objects from the database.
	 * 
	 * Should be used for development and testing, not production.
	 * 
	 * @return
	 */
//	public void deleteURLSets() {
//		Set<String> keys = urlSetKeys();
//		Transaction t = jedis.multi();
//		for (String key: keys) {
//			t.del(key);
//		}
//		t.exec();
//	}

	/**
	 * Deletes all URLSet objects from the database.
	 * 
	 * Should be used for development and testing, not production.
	 * 
	 * @return
	 */
//	public void deleteTermCounters() {
//		Set<String> keys = termCounterKeys();
//		Transaction t = jedis.multi();
//		for (String key: keys) {
//			t.del(key);
//		}
//		t.exec();
//	}

	/**
	 * Deletes all keys from the database.
	 * 
	 * Should be used for development and testing, not production.
	 * 
	 * @return
	 */
//	public void deleteAllKeys() {
//		Set<String> keys = jedis.keys("*");
//		Transaction t = jedis.multi();
//		for (String key: keys) {
//			t.del(key);
//		}
//		t.exec();
//	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		SQLDatabase jedis = new SQLDatabase();
		JedisIndex index = new JedisIndex(jedis);
		
//		index.deleteTermCounters();
//		index.deleteURLSets();
//		index.deleteAllKeys();
		loadIndex(index);
		
		Map<String, Integer> map = index.getCounts("the");
		for (Entry<String, Integer> entry: map.entrySet()) {
			System.out.println(entry);
		}
	}

	/**
	 * Stores two pages in the index for testing purposes.
	 * 
	 * @return
	 * @throws IOException
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	private static void loadIndex(JedisIndex index) throws IOException, ClassNotFoundException, SQLException {
		WikiFetcher wf = new WikiFetcher();

		String url = "https://en.wikipedia.org/wiki/Java_(programming_language)";
		Elements paragraphs = wf.readWikipedia(url);
		index.indexPage(url, paragraphs);
		
		url = "https://en.wikipedia.org/wiki/Programming_language";
		paragraphs = wf.readWikipedia(url);
		index.indexPage(url, paragraphs);
	}
}
