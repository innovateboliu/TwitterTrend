TwitterTrend
============

Real time analysis showing the most popular # on Twitter about big data

Using Apache Storm

mvn clean compile assembly:single

storm jar target/twitter-trend-1.0-SNAPSHOT-jar-with-dependencies.jar com.bo.TwitterTrend | tee log
