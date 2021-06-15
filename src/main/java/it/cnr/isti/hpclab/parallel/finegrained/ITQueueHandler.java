package it.cnr.isti.hpclab.parallel.finegrained;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ITQueueHandler {
	protected IntersectionTask[] queue;
	protected AtomicInteger threadsDesired;
	protected AtomicInteger nextElement;
	protected AtomicBoolean free;
	
	public ITQueueHandler() {
		threadsDesired = new AtomicInteger(0);
		nextElement = new AtomicInteger(0);
		free = new AtomicBoolean(true);
	}
	
	private void freeQueue() {
		queue = null;
		free.set(true);
	}
	
	public boolean setQueue(IntersectionTask[] queue, int numThreads) {
		if(free.compareAndSet(true, false)) {
			this.queue = queue;
			nextElement.set(0);
			threadsDesired.set(numThreads);
			return true;
		}
		return false;
	}
	
	public void decrementThreads() {
		if(threadsDesired.decrementAndGet() == 0)
			freeQueue();
	}
	
	public IntersectionTask getTask() {
		int index = nextElement.getAndIncrement();
		if(index < queue.length) {
			return queue[index];
		}
		return null;
	}
}
