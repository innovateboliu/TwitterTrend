import java.io.Serializable;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;


public class TwitterTrendUtils {
	public static class  Pair<T, V> implements Serializable, Comparable<Pair<T, V>>{
		 T first;
		 V second;
		
		public Pair(T first, V second) {
			this.first = first;
			this.second = second;
		}
		
		@Override
		public int hashCode() {
			return first.hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			return first.equals(((Pair<T, V>)obj).first);
		}

		@Override
		public int compareTo(Pair<T, V> o) {
			return (Integer)this.second - (Integer)o.second;
		}
	}
	public static interface CompAndSeri <T> extends Comparable<T>, Serializable{
		
	}
	public static boolean isTickTuple(Tuple tuple) {
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
				&& tuple.getSourceStreamId().equals(
						Constants.SYSTEM_TICK_STREAM_ID);
	}
}
