package com.bo;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import redis.clients.jedis.Jedis;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.bo.TwitterTrendUtils.Pair;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class HashtagFinalRankBolt extends BaseBasicBolt {
	private static final long serialVersionUID = -6355552661569011361L;
	private static final int DEFAULT_QUERY_FREQUENCY_IN_SECONDS = 2;
	private static final int DEFAULT_COUNT = 20;
	private PriorityQueue<TwitterTrendUtils.Pair<String, Integer>> pq = new PriorityQueue<TwitterTrendUtils.Pair<String, Integer>>(
			DEFAULT_COUNT);
	private PrintWriter writer;
	Jedis jedis ;
	
	@Override
    public void prepare(Map stormConf, TopologyContext context) {
		try {
			writer = new PrintWriter("log/finalRankBolt.txt", "UTF-8");
			
			// Nodejitsu redis server
//			jedis = new Jedis("nodejitsudb4112456240.redis.irstack.com", 6379);
//			jedis.auth("nodejitsudb4112456240.redis.irstack.com:f327cfe980c971946e80b8e975fbebb4");
			
			//Heroku redis to go server
//			jedis = new Jedis("pearlfish.redistogo.com", 9601);
//			jedis.auth("eade05d17a0cd9d29fa5933894412ea7");
			
			//Heroku redis cloud server
			jedis = new Jedis("pub-redis-18039.us-east-1-4.2.ec2.garantiadata.com", 18039);
			jedis.auth("oB7WsFKMNO2MNfzr");
			
			
			jedis.connect();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
    }
	
	@Override
	public void cleanup() {
		writer.close();
	}
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		
		if (TwitterTrendUtils.isTickTuple(tuple)) {
			writer.println("fetch-------------------------------");
			
			List<TwitterTrendUtils.Pair<String, Integer>> list = new ArrayList<TwitterTrendUtils.Pair<String,Integer>>();
			List<Map<String,String>> rankingList = new ArrayList<Map<String,String>>();
			try {
				Iterator<TwitterTrendUtils.Pair<String, Integer>> pqIter = pq.iterator();
				while (pqIter.hasNext()) {
					list.add(pqIter.next());
					pqIter.remove();
				}
				for (int i = list.size() - 1; i >= 0; i--) {
					writer.println(list.get(i).first + " " + list.get(i).second);
					Map<String, String> item = new HashMap<String, String>();
					item.put("href", "https://twitter.com/search?q=%23"+list.get(i).first + "&src=tyah");
					item.put("id", list.get(i).first);
					item.put("cnt", list.get(i).second.toString());
					rankingList.add(item);
				}
				Gson gson = new GsonBuilder().create();
				jedis.publish("twitter_trend", gson.toJson(rankingList));
			}catch (ConcurrentModificationException e) {
				writer.println("ConcurrentModificationException up!!!");
				throw e;
			}
			writer.flush();
		} else {
			PriorityQueue<Pair<String, Integer>> newPq = (PriorityQueue<Pair<String, Integer>>) tuple.getValue(0);
			try {
				for (Pair<String, Integer> oldPair : newPq) {
					Pair<String, Integer> pair = new Pair<String, Integer>(oldPair.first, oldPair.second);
					if (pq.contains(pair)) {
						pq.remove(pair);
					}
					pq.add(pair);
					if (pq.size() > DEFAULT_COUNT) {
						pq.remove();
					}
				}
			}catch (ConcurrentModificationException e) {
				writer.println("ConcurrentModificationException!!!");
				throw e;
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("rankedList"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,
				DEFAULT_QUERY_FREQUENCY_IN_SECONDS);
		return conf;
	}

}

