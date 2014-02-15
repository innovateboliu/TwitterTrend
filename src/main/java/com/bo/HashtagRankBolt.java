package com.bo;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.bo.TwitterTrendUtils.Pair;

public class HashtagRankBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 4481610288723006295L;
	private static final int DEFAULT_QUERY_FREQUENCY_IN_SECONDS = 2;
	private static final int DEFAULT_COUNT = 20;
	private PriorityQueue<Pair<String, Integer>> pq = new PriorityQueue<TwitterTrendUtils.Pair<String, Integer>>(
			DEFAULT_COUNT);
	private PrintWriter writer;

	@Override
    public void prepare(Map stormConf, TopologyContext context) {
		try {
			writer = new PrintWriter("log/rankBolt.txt", "UTF-8");
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
			try {
				for (Pair<String, Integer> pair : pq) {
					writer.println(pair.first + " " + pair.second);
				}
			}catch (ConcurrentModificationException e) {
				writer.println("ConcurrentModificationException up!!!");
				throw e;
			}
			writer.flush();
			PriorityQueue<Pair<String, Integer>> copyPq = new PriorityQueue<Pair<String,Integer>>();
			for (Pair<String, Integer> pair : pq) {
				copyPq.add(new Pair<String, Integer>(pair.first, pair.second));
			}
			collector.emit(new Values(copyPq));

		} else {
			String key = (String) tuple.getValue(0);
			int value = (Integer) tuple.getValue(1);
			TwitterTrendUtils.Pair<String, Integer> newPair = new TwitterTrendUtils.Pair<String, Integer>(key, value);
			try {
				if (pq.contains(newPair)) {
					pq.remove(newPair);
				}
				if (key != null) {
					pq.add(new TwitterTrendUtils.Pair<String, Integer>(key, value));
					if (pq.size() > DEFAULT_COUNT) {
						pq.remove();
					}
				}
			} catch (ConcurrentModificationException e) {
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

