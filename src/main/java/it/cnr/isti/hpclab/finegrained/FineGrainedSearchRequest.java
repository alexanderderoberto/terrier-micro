package it.cnr.isti.hpclab.finegrained;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.terrier.structures.LexiconEntry;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.matching.structures.QueryProperties.RuntimeProperty;
import it.cnr.isti.hpclab.matching.structures.Result;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.matching.structures.query.QueryTerm;
import it.unimi.dsi.fastutil.PriorityQueue;

public class FineGrainedSearchRequest {
	// If srq is null, this message is a "poison pill", and the thread receiving it must terminate.
	public final SearchRequest srq;
	public ArrayList<QueryTerm> terms; //Accessed concurrently by many threads, but read only!
	public ArrayList<LexiconEntry> lexicons; //Accessed concurrently by many threads, but read only!
	protected final int numberOfTasks;
	/** Array used to contain intersection tasks */
	private IntersectionTask[] mIntersectionTasks;
	/** Used to insert intersection tasks into correct positions in the array */
	private int intersectionTastPointer;
	/** Used to store the results */
	public final ConcurrentTopQueue heap;
	private AtomicInteger tasksCompleted;
	
	private long start_time;
	
	public long processingTime;
	public long processedPostings;
	public float initialThreshold;
	
	public FineGrainedSearchRequest(final SearchRequest srq, ArrayList<QueryTerm> terms, ArrayList<LexiconEntry> lexicons, int numberOfTasks)
	{
		TinyJProfiler.tic();
		
		this.start_time = System.nanoTime();
		
		this.srq = srq;
		this.terms = terms;
		this.lexicons = lexicons;
		this.numberOfTasks = numberOfTasks;
		
		this.mIntersectionTasks = new IntersectionTask[numberOfTasks];
		this.intersectionTastPointer = 0;
		initialThreshold = parseFloat(srq.getQuery().getMetadata(RuntimeProperty.INITIAL_THRESHOLD));
		this.heap = new ConcurrentTopQueue(MatchingConfiguration.getInt(Property.TOP_K), initialThreshold);
		this.tasksCompleted = new AtomicInteger();
		
		this.processedPostings = 0l;
		TinyJProfiler.toc();
	}
	
	public void addIntersectionTask(IntersectionTask task){
		TinyJProfiler.tic();
		mIntersectionTasks[intersectionTastPointer++] = task;
		TinyJProfiler.toc();
	}
	
	public long getProcessingTime(){
		return processingTime;
	}
	
	public boolean isCompleted(){
		TinyJProfiler.tic();
		if(tasksCompleted.incrementAndGet() == numberOfTasks){
			processingTime = System.nanoTime() - start_time;
			TinyJProfiler.toc();
			return true;
		}
		TinyJProfiler.toc();
		return false;
	}
	
	public boolean isPoison()
	{
		return this.srq == null;
	}
	
	//TODO: provare ad usare un solo heap da tutti i thread in modalità concorrente (quindi senza merge finale)
	/*
	public TopQueue merge(){
		TinyJProfiler.tic();
		
		TopQueue heap = new TopQueue(MatchingConfiguration.getInt(Property.TOP_K));
		PriorityQueue<Result> currentTasksQ;
		for(int i=0; i<numberOfTasks; i++){
			// Sum up the number of processed postings
			processedPostings += mIntersectionTasks[i].processedPostings;
			
			// Merge heaps
			currentTasksQ = mIntersectionTasks[i].heap.top();
			while(!currentTasksQ.isEmpty()){
				heap.insert(currentTasksQ.dequeue());
			}
		}
		
		TinyJProfiler.toc();
		return heap;
	}
	*/

	public static float parseFloat(String s)
	{
		if (s == null)
			return 0.0f;
		return Float.parseFloat(s);
	}
}
