package com.flatironschool.javacs;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import redis.clients.jedis.Jedis;


/**
 * Represents the results of a search query.
 *
 */
public class WikiSearch {
	
	// map from URLs that contain the term(s) to relevance score
	private Map<String, Integer> map;

	/**
	 * Constructor.
	 * 
	 * @param map
	 */
	public WikiSearch(Map<String, Integer> map) {
		this.map = map;
	}
	
	/**
	 * Looks up the relevance of a given URL.
	 * 
	 * @param url
	 * @return
	 */
	public Integer getRelevance(String url) {
		Integer relevance = map.get(url);
		return relevance==null ? 0: relevance;
	}
	
	/**
	 * Prints the contents in order of term frequency.
	 * 
	 * @param map
	 */
	private  void print() {
		List<Entry<String, Integer>> entries = sort();
		for (Entry<String, Integer> entry: entries) {
			System.out.println(entry);
		}
	}
	
	/**
	 * Computes the union of two search results.
	 * 
	 * @param that
	 * @return New WikiSearch object.
	 */
	public WikiSearch or(WikiSearch that) {
		WikiSearch result = new WikiSearch(this.map);
		for(String url: that.map.keySet()) {
			if(result.map.containsKey(url)) {
				result.map.put(url, result.map.get(url) + that.map.get(url));
			}
			else {
				result.map.put(url, that.map.get(url));
			}
		}
		return result;
	}
	
	/**
	 * Computes the intersection of two search results.
	 * 
	 * @param that
	 * @return New WikiSearch object.
	 */
	public WikiSearch and(WikiSearch that) {
		Map <String, Integer> space = new HashMap <String, Integer>();
		for(String url: this.map.keySet()) {
			if(this.map.containsKey(url) && that.map.containsKey(url)) {
				space.put(url, that.map.get(url) + this.map.get(url));
			}
		}
		WikiSearch result = new WikiSearch(space);
		return result;
	}
	
	/**
	 * Computes the intersection of two search results.
	 * 
	 * @param that
	 * @return New WikiSearch object.
	 */
	public WikiSearch minus(WikiSearch that) {
		WikiSearch result = new WikiSearch(this.map);
		for(String url: that.map.keySet()) {
			if(this.map.containsKey(url)) {
				result.map.remove(url);
			}
		}
		return result;
	}
	
	/**
	 * Computes the relevance of a search with multiple terms.
	 * 
	 * @param rel1: relevance score for the first search
	 * @param rel2: relevance score for the second search
	 * @return
	 */
	protected int totalRelevance(Integer rel1, Integer rel2) {
		// simple starting place: relevance is the sum of the term frequencies.
		return rel1 + rel2;
	}

	/**
	 * Sort the results by relevance.
	 * 
	 * @return List of entries with URL and relevance.
	 */
	public List<Entry<String, Integer>> sort() {
		List<Integer> sorter = new ArrayList<Integer>();
		List<Entry<String, Integer>> result = new ArrayList<Entry<String, Integer>>();
		for(Integer value: map.values()) {
			sorter.add(value);
			result.add(null);
		}
		java.util.Collections.sort(sorter);
		for(Entry<String, Integer> entry: map.entrySet()) {
			result.set(sorter.indexOf(entry.getValue()), entry);
		}
		return result;
	}

	/**
	 * Performs a search and makes a WikiSearch object.
	 * 
	 * @param term
	 * @param index
	 * @return
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	public static WikiSearch search(String term, JedisIndex index) throws ClassNotFoundException, SQLException {
		Map<String, Integer> map = index.getCounts(term);
		return new WikiSearch(map);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		
		// make a JedisIndex
		SQLDatabase jedis = new SQLDatabase();
		JedisIndex index = new JedisIndex(jedis); 
		
		// search for the first term
		String term1 = "java";
		System.out.println("Query: " + term1);
		WikiSearch search1 = search(term1, index);
		search1.print();
		
		// search for the second term
		String term2 = "programming";
		System.out.println("Query: " + term2);
		WikiSearch search2 = search(term2, index);
		search2.print();
		
		// compute the intersection of the searches
		System.out.println("Query: " + term1 + " AND " + term2);
		WikiSearch intersection = search1.and(search2);
		intersection.print();
	}
}
