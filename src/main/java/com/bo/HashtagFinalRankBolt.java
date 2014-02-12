package com.bo;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
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

public class HashtagFinalRankBolt extends BaseBasicBolt {
	private static final long serialVersionUID = -6355552661569011361L;
	private static final int DEFAULT_QUERY_FREQUENCY_IN_SECONDS = 2;
	private static final int DEFAULT_COUNT = 20;
	private PriorityQueue<TwitterTrendUtils.Pair<String, Integer>> pq = new PriorityQueue<TwitterTrendUtils.Pair<String, Integer>>(
			DEFAULT_COUNT);
	private PrintWriter writer;
	
	@Override
    public void prepare(Map stormConf, TopologyContext context) {
		try {
			writer = new PrintWriter("log/finalRankBolt.txt", "UTF-8");
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
			collector.emit(new Values(pq));
			List<TwitterTrendUtils.Pair<String, Integer>> list = new ArrayList<TwitterTrendUtils.Pair<String,Integer>>();
			try {
				while (pq.size() > 0) {
					list.add(pq.remove());
				}
				for (int i = list.size() - 1; i >= 0; i--) {
					writer.println(list.get(i).first + " " + list.get(i).second);
				}
			}catch (ConcurrentModificationException e) {
				writer.println("ConcurrentModificationException up!!!");
				throw e;
			}
//			for (TwitterTrendUtils.Pair<String, Integer> pair : pq) {
//				writer.println(pair.first + " " + pair.second);
//			}
			writer.flush();
		} else {
			PriorityQueue<TwitterTrendUtils.Pair<String, Integer>> newPq = (PriorityQueue<TwitterTrendUtils.Pair<String, Integer>>) tuple.getValue(0);
			try {
				for (TwitterTrendUtils.Pair<String, Integer> pair : newPq) {
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

