package it.cnr.isti.hpclab.parallel.finegrained;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;

public class ResourceManager {
	protected final int numThreads;
	protected Map<String, Integer> threadToQueue;
	
	//Queues
	protected final ITQueueHandler[] ITQueues;
	
	//Threads
	private LinkedList<Thread> idleThreadsFIFO = new LinkedList<Thread>();
	private Object[] workersLocks;
	
	public ResourceManager(int numThreads){
		this.numThreads = numThreads;
		threadToQueue = new HashMap(); //It is also used as lock for the producer
		
		//Initialise intersection tasks' queues and workersLocks
		ITQueues = new ITQueueHandler[numThreads];
		//Independent locks are used for queues to minimize the overhead
		workersLocks = new Object[numThreads];
		for(int i=0; i<numThreads; i++) {
			ITQueues[i] = new ITQueueHandler();
			workersLocks[i] = new Object();
		}
		
	}
	
	public void addWorkerThread(Thread th) {
		synchronized(threadToQueue) {
			threadToQueue.put(th.getName(), -1);
			threadToQueue.notify();
		}
	}
	
	public int bindITQueue(IntersectionTask[] queue, int numAssignedThreads, int queryId) {
		try {
			int result = 0;
			boolean condition = false;
			while(true) {
				synchronized(threadToQueue) {
					if(idleThreadsFIFO.size() > 0) {
						for(int i=0; i<numThreads; i++) {
							if(ITQueues[i].setQueue(queue, numAssignedThreads)) {
								result = i;
								condition = true;
								break;
							}
						}
					}
					if(!condition)
						threadToQueue.wait();
				}
				if(condition)
					break;
			}
			return result;
		} catch(InterruptedException e) {
			e.printStackTrace();
			System.exit(0);
			return 0;
		}
	}
	
	public void bindThreadToQueue(int queueId) {
		try {
			boolean condition = false;
			while(true) {
				synchronized(threadToQueue) {
					if(idleThreadsFIFO.size() > 0) {
						WorkerThread th = (WorkerThread)idleThreadsFIFO.removeFirst();
						threadToQueue.replace(th.getName(), queueId);
						synchronized(workersLocks[th.id - 1]){
							workersLocks[th.id - 1].notify();
						}
						condition = true;
					}
					else
						threadToQueue.wait();
				}
				if(condition)
					break;
			}
		} catch(InterruptedException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	public IntersectionTask takeTask(Thread th) {
		int queueId = threadToQueue.get(th.getName());
		if(queueId >= 0) {
			return ITQueues[queueId].getTask();
		}
		
		try {
			boolean condition = false;
			while(true) {
				synchronized(threadToQueue) {
					queueId = threadToQueue.get(th.getName());
					if(queueId >= 0) {
						condition = true;
					}
					else { // Put thread back in the idleThreadFIFO because it is not assigned to any queue
						idleThreadsFIFO.add(th);
						threadToQueue.notify();
					}
				}
				if(condition)
					break;
				
				// Lock the thread because it is not assigned to any queue
				synchronized(workersLocks[((WorkerThread)th).id - 1]){
					if(threadToQueue.get(th.getName()) < 0) { // Lock avoidance!
						workersLocks[((WorkerThread)th).id - 1].wait();
					}
				}
			}
			return ITQueues[queueId].getTask();
		} catch(InterruptedException e) {
			e.printStackTrace();
			System.exit(0);
			return null;
		}
	}
	
	public void unbindThreadFromQueue(Thread th) {
		synchronized(threadToQueue) {
			int queueId = threadToQueue.get(th.getName());
			if(queueId < 0){
				System.err.println("ERROR: An attempt to unbind an already unbound thread was made");
				System.exit(0);
			}
			threadToQueue.replace(th.getName(), -1);
			ITQueues[queueId].decrementThreads();
		}
	}
}