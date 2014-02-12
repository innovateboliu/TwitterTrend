package com.bo;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class HashtagCountBolt extends BaseRichBolt {
	private static final long serialVersionUID = 975184103612255919L;
	private OutputCollector collector;
	private int queryFrequencyInSeconds;
	private int slideWindowLengthInSeconds;
	private SlideWindowCounter<HashTag> counter = null;
	private PrintWriter writer;

	public HashtagCountBolt(int slideWindowLengthInSeconds,
			int queryFrequencyInSeconds) {
		this.slideWindowLengthInSeconds = slideWindowLengthInSeconds;
		this.queryFrequencyInSeconds = queryFrequencyInSeconds;
		counter = new SlideWindowCounter<HashTag>(
				this.slideWindowLengthInSeconds / this.queryFrequencyInSeconds);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		try {
			writer = new PrintWriter("log/countBolt.txt", "UTF-8");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple tuple) {
		
		if (TwitterTrendUtils.isTickTuple(tuple)) {
			writer.println("fetch-------------------------------");
			Map<HashTag, Integer> counts = counter.getWindowCounts();
			for (Entry<HashTag, Integer> entry : counts.entrySet()) {
				 collector.emit(new Values(entry.getKey().getContent(), entry.getValue()));
				 writer.println(entry.getKey() + " " + entry.getValue());
			}
			writer.flush();
		} else {
			counter.inc(new HashTag((String)tuple.getValue(0)), 1);
			collector.ack(tuple);
		}
	}

	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("hashtag", "count"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, queryFrequencyInSeconds);
		return conf;
	}

}

