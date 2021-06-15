package it.cnr.isti.hpclab.parallel.finegrained;

public class RateLimiter {
	private static long ms = 1000000;
	private final long interval;
	private long nextExecution;

	// desiredRate: desired QPS (query per second)
	public RateLimiter(int desiredRate) {
		if(desiredRate == 0)
			interval = 0;
		else
			interval = (1 * 1000 * 1000 * 1000) / desiredRate;
		
		nextExecution = System.nanoTime();
	}
	
	public void await() throws InterruptedException {
		if(interval == 0)
			return;
		
		long delta = nextExecution - System.nanoTime();
		if(delta > ms) { // greater than 1ms.
			Thread.sleep(delta / ms);
		}
		while(nextExecution - System.nanoTime() > 0) {
			;
		}
		nextExecution += interval;
	}
}
