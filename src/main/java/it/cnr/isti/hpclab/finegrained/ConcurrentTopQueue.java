package it.cnr.isti.hpclab.finegrained;

import it.cnr.isti.hpclab.matching.structures.Result;
import it.cnr.isti.hpclab.matching.structures.TopQueue;

public class ConcurrentTopQueue {
	private TopQueue queue;
	
	public ConcurrentTopQueue(final int k, final float th){
		this.queue = new TopQueue(k, th);
	}
	
	public synchronized TopQueue getQueue() {
		return queue;
	}
	
	public synchronized boolean insert(Result c) {
		return queue.insert(c);
	}
	
	public synchronized boolean isEmpty() {
		return queue.isEmpty();
	}
	
	public synchronized int size() {
		return queue.size();
	}
	
	public synchronized float threshold() {
		return queue.threshold();
	}
}