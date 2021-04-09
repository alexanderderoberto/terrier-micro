package it.cnr.isti.hpclab.parallel.finegrained;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.terrier.structures.LexiconEntry;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.manager.MatchingEntry;
import it.cnr.isti.hpclab.matching.structures.QueryProperties.RuntimeProperty;
import it.cnr.isti.hpclab.matching.structures.Result;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.matching.structures.TopQueue;
import it.cnr.isti.hpclab.matching.structures.query.QueryTerm;
import it.unimi.dsi.fastutil.PriorityQueue;

public class FineGrainedSearchRequest {
	/** Underlying search request */
	public final SearchRequest srq; // If srq is null, this message is a "poison pill", and the thread receiving it must terminate.
	/** Query terms and "properties" of those terms */
	public final ArrayList<QueryTerm> terms; // Accessed concurrently by many threads, but read only!
	public final ArrayList<LexiconEntry> lexicons; // Accessed concurrently by many threads, but read only!
	/** Number of chunks the search request was divided into */
	protected final int numberOfTasks;
	/** The moment in time when the processing of this search request has begun (which is antecedent w.r.t. this object) */
	private final long startTime;
	/** Array used to contain intersection tasks */
	private IntersectionTask[] mIntersectionTasks;
	/** Used to insert intersection tasks into correct positions in the array */
	private int intersectionTastPointer;
	/** Used to detect when all chunks of the search request are ready */
	private AtomicInteger tasksCompleted;
	
	public long processingTime;
	public long processedPostings;
	public float initialThreshold;
	
	public FineGrainedSearchRequest(final SearchRequest srq, ArrayList<QueryTerm> terms, ArrayList<LexiconEntry> lexicons, int numberOfTasks, long startTime)
	{
		this.srq = srq;
		this.terms = terms;
		this.lexicons = lexicons;
		this.numberOfTasks = numberOfTasks;
		this.startTime = startTime;
		
		this.mIntersectionTasks = new IntersectionTask[numberOfTasks];
		this.intersectionTastPointer = 0;
		this.tasksCompleted = new AtomicInteger();
		
		this.processedPostings = 0L;
		this.initialThreshold = parseFloat(srq.getQuery().getMetadata(RuntimeProperty.INITIAL_THRESHOLD));
	}
	
	public void addIntersectionTask(IntersectionTask task)
	{
		mIntersectionTasks[intersectionTastPointer++] = task;
	}
	
	/** Adds to the request object the statistics about its processing */
	public void addStatistics(List<MatchingEntry> enums, TopQueue heap)
	{
		srq.getQuery().addMetadata(RuntimeProperty.QUERY_TERMS, Arrays.toString(enums.stream().map(x -> "\"" + x.term + "\"").collect(Collectors.toList()).toArray()));
		srq.getQuery().addMetadata(RuntimeProperty.QUERY_LENGTH,    Integer.toString(enums.size()));
		srq.getQuery().addMetadata(RuntimeProperty.PROCESSED_TERMS_DF, Arrays.toString(enums.stream().map(x -> x.entry.getDocumentFrequency()).collect(Collectors.toList()).toArray()));
		srq.getQuery().addMetadata(RuntimeProperty.FINAL_THRESHOLD,    Float.toString(heap.threshold()));
        srq.getQuery().addMetadata(RuntimeProperty.INITIAL_THRESHOLD,  Float.toString(initialThreshold));
		srq.getQuery().addMetadata(RuntimeProperty.NUM_RESULTS, 	     Integer.toString(heap.size()));
		srq.getQuery().addMetadata(RuntimeProperty.PROCESSED_POSTINGS, Long.toString(processedPostings));
		srq.getQuery().addMetadata(RuntimeProperty.PROCESSING_TIME,    Double.toString(processingTime/1e6));
	}
	
	/** Increments the counter of completed chunks and returns true if all the chunks of this search request has been processed, false otherwise */
	public boolean isCompleted()
	{
		return tasksCompleted.incrementAndGet() == numberOfTasks;
	}
	
	public boolean isPoison()
	{
		return this.srq == null;
	}
	
	/** Merges the heaps of all chunks belonging to this search request into a single heap and returns it */
	public TopQueue merge()
	{
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
		
		processingTime = System.nanoTime() - startTime;
		
		return heap;
	}

	public static float parseFloat(String s)
	{
		if (s == null)
			return 0.0f;
		return Float.parseFloat(s);
	}
}
