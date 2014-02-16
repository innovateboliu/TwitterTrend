package com.bo;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TwitterTrend {
	private TopologyBuilder builder;
	private String topoName;
	private Config topoConfig;
//	private int runtimeInSeconds;

	public TwitterTrend() throws InterruptedException {
	    builder = new TopologyBuilder();
	    topoName = "slidingWindowCounts";
	    topoConfig = new Config();
	    topoConfig.setDebug(true);
//	    runtimeInSeconds = 1000000000;

		setup();
	  }
	
	public void setup() {
		String spoutId = "hashtagProducer";
	    String counterId = "counter";
	    String rankerId = "ranker";
	    String finalRankerId = "finalRanker";
	    builder.setSpout(spoutId, new HashtagSpout());
	    builder.setBolt(counterId, new HashtagCountBolt(1200, 2), 4).fieldsGrouping(spoutId, new Fields("hashtag"));
	    builder.setBolt(rankerId, new HashtagRankBolt(), 4).fieldsGrouping(counterId, new Fields("hashtag"));
	    builder.setBolt(finalRankerId, new HashtagFinalRankBolt()).globalGrouping(rankerId);
	}
	
	public void run() throws InterruptedException {
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topoName, topoConfig, builder.createTopology());
//		Thread.sleep((long) runtimeInSeconds * 1000);
//		cluster.killTopology(topoName);
//		cluster.shutdown();
	}
	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
		TwitterTrend ins = new TwitterTrend();
		ins.run();
	}
	
	

}

