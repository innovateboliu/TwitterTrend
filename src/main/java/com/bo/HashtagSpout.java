package com.bo;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.FilterQuery;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class HashtagSpout extends BaseRichSpout {

	private static final long serialVersionUID = -6836430586497922562L;
	SpoutOutputCollector collector;
    LinkedBlockingQueue<String> queue = null;
    TwitterStream twitterStream;
    private static final String CONSUMER_KEY = "2HVsE0qtjUy2qFvAf3WKw";
    private static final String CONSUMER_SECRET = "Cik89sq81VC1q3WE5Mw03MJnU44xAUt47UEA6XcO2U";
    private static final String ACCESS_TOKEN = "35779544-ai5ylbP4KOn3pQs7NzGNHZAKfHETRn0CmdRZN9gpI";
    private static final String ACCESS_TOKEN_SECRET = "UnzR6FdCvPikrnkp33nI80STPbHfShfypuePfn18";
    private static final String[] KEY_WORDS = { 
    	"hadoop", "big data", "bigdata", "cloudera", "data science", "data scientiest", 
    	"business intelligence", "mapreduce", "data warehouse", "data warehousing", "apache mahout",
    	"hbase", "nosql", "newsql", "businessintelligence", "cloudcomputing", "apache storm", "data engineer", "apache spark", "impala", "klout", "apache flume"};

    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        queue = new LinkedBlockingQueue<String>(1000000);
        this.collector = collector;
        StatusListener listener = new StatusListener() {

            @Override
            public void onStatus(Status status) {
            	HashtagEntity[] hashtags = status.getHashtagEntities();
            	for (HashtagEntity hashtag: hashtags) {
            		queue.offer(hashtag.getText());
            	}
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            @Override
            public void onTrackLimitationNotice(int i) {
            }

            @Override
            public void onScrubGeo(long l, long l1) {
            }

            @Override
            public void onException(Exception e) {
            }

			@Override
			public void onStallWarning(StallWarning arg0) {
			}
            
        };
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey(CONSUMER_KEY);
        cb.setOAuthConsumerSecret(CONSUMER_SECRET);
        cb.setOAuthAccessToken(ACCESS_TOKEN);
        cb.setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET);
        cb.setJSONStoreEnabled(true);
        cb.setIncludeEntitiesEnabled(true);

        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        twitterStream.addListener(listener);
        twitterStream.filter(new FilterQuery().track(KEY_WORDS));
    }

    @Override
    public void nextTuple() {
        String ret = queue.poll();
        if(ret==null) {
            Utils.sleep(50);
        } else {
            this.collector.emit(new Values(ret));
        }
    }

    @Override
    public void close() {
        twitterStream.shutdown();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }    

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag"));
    }
    
}

