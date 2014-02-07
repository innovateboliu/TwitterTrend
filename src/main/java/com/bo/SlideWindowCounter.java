package com.bo;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class SlideWindowCounter <T> implements Serializable{
	private static final long serialVersionUID = -1915470651421889175L;
	private int slotsNum;
	private int cnt;
	private Map<T, List<Integer>> counter = new ConcurrentHashMap <T, List<Integer>>();
	private int cur;
	
	public SlideWindowCounter(int slotsNum) {
		if (slotsNum < 2) {
			throw new IllegalArgumentException("Number of slots should be at least than 2");
		}
		this.slotsNum = slotsNum;
		cur = 0;
	}
	
	public void inc(T key, int num) {
		if (!counter.containsKey(key)) {
			counter.put(key, new ArrayList<Integer>(Collections.nCopies(slotsNum, 0)));
		}
		int curNum = counter.get(key).get(cur);
		counter.get(key).set(cur, curNum + num);
	}
	
	public Map<T, Integer> getWindowCounts() {
		if (cnt < slotsNum) {
			cnt++;
		}
		
		int next = (cur + 1) % slotsNum;
		cur = next;
		
		Map<T, Integer> result = new HashMap<T, Integer>();
		for (Entry<T, List<Integer>> entry : counter.entrySet()) {
			T key = entry.getKey();
			List<Integer> counts = counter.get(key);
			if (counts == null) {
				continue;
			}
			int sum = 0;
			for (Integer i : counts) {
				sum += i;
			}
			result.put(key, sum);
			if (cnt == slotsNum) {
				counts.set(cur, 0);
			}
		}
		
		
		
		return result;
	}
	
	
	
	public static void main(String[] args) {
		SlideWindowCounter<String> ins = new SlideWindowCounter<String>(3);
		ins.inc("a", 1);
		ins.inc("b", 1);
		ins.inc("a", 1);
		ins.inc("b", 1);
		
		Map<String, Integer> result = ins.getWindowCounts();
		
		ins.inc("a", 1);
		ins.inc("b", 1);
		ins.inc("a", 1);
		ins.inc("b", 1);
		
		result = ins.getWindowCounts();
		Map<String, List<Integer>> map = ins.counter;
		
		ins.inc("a", 1);
		ins.inc("b", 1);
		ins.inc("a", 1);
		ins.inc("b", 1);
		
		result = ins.getWindowCounts();
		map = ins.counter;
		
		ins.inc("a", 1);
		ins.inc("b", 1);
		result = ins.getWindowCounts();
		map = ins.counter;
		
	}
}

