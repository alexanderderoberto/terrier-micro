package it.cnr.isti.hpclab.finegrained;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.matching.structures.QueryProperties.RuntimeProperty;
import it.cnr.isti.hpclab.matching.structures.Result;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.matching.structures.TopQueue;
import it.unimi.dsi.fastutil.PriorityQueue;

public class FineGrainedSearchRequest {
	// If srq is null, this message is a "poison pill", and the thread receiving it must terminate.
	public final SearchRequest srq;
	private List<SkipList> mSkipLists;
	private List<IntersectionTask> mIntersectionTasks;
	private AtomicInteger tasksCompleted;
	private long start_time;
	
	public long processingTime;
	public long processedPostings;
	public float initialThreshold;
	
	private int mIntersectionTasksCreated;
	
	public FineGrainedSearchRequest(final SearchRequest srq)
	{
		this.srq = srq;
		this.mSkipLists = new ArrayList();
		this.mIntersectionTasks = new ArrayList<IntersectionTask>();
		this.tasksCompleted = new AtomicInteger();
		
		initialThreshold = parseFloat(srq.getQuery().getMetadata(RuntimeProperty.INITIAL_THRESHOLD));
		
		this.processedPostings = 0l;
	}
	
	public void addIntersectionTask(IntersectionTask task){
		mIntersectionTasks.add(task);
	}
	
	public long getProcessingTime(){
		return processingTime;
	}
	
	public List<SkipList> getSkipLists() {
		return mSkipLists;
	}
	
	public boolean isCompleted(){
		if(tasksCompleted.incrementAndGet() == mSkipLists.get(0).size()){
			processingTime = System.nanoTime() - start_time;
			return true;
		}
		return false;
	}
	
	public boolean isPoison()
	{
		return this.srq == null;
	}
	
	//TODO: provare ad usare un solo heap da tutti i thread in modalità concorrente (quindi senza merge finale)
	public TopQueue merge(){
		ArrayList<Integer> tempArray = new ArrayList();
		
		TopQueue heap = new TopQueue(MatchingConfiguration.getInt(Property.TOP_K));
		PriorityQueue<Result> currentTasksQ;
		for(int i=0; i<mIntersectionTasks.size(); i++){
			//Incremento il numero di postings analizzati
			processedPostings += mIntersectionTasks.get(i).processedPostings;
			
			// Unisco i heap in uno
			currentTasksQ = mIntersectionTasks.get(i).getHeap().top();
			while(!currentTasksQ.isEmpty()){
				//Inizio eliminazione doppioni temporanea
				Result r = currentTasksQ.dequeue();
				if(!tempArray.contains(r.getDocId())){
					tempArray.add(r.getDocId());
					heap.insert(r);
				}
				//Fine eliminazione doppioni temporanea
				//TODO: eliminare ciò che sta su e decommentare la riga sotto
				//heap.insert(currentTasksQ.dequeue());
			}
		}
		return heap;
	}

	public static float parseFloat(String s)
	{
		if (s == null)
			return 0.0f;
		return Float.parseFloat(s);
	}
	
	// Adds skip lists to the request data structure in ordered manner (by skip list's lenght)
	public void putSkipList(int index, SkipList newList) {
		for(int i=0; i<mSkipLists.size(); i++) {
			if(mSkipLists.get(i).size() >= newList.size()) {
				mSkipLists.add(i, newList);
				return;
			}
		}
		mSkipLists.add(newList);
	}
	
	public void startTime(){
		this.start_time = System.nanoTime();
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Skip lists: (");
		for(SkipList skipList: mSkipLists) {
			sb.append(skipList.size() + ", ");
		}
		sb.setLength(sb.length() - 2);
		sb.append(")\n");
		sb.append("Intersection tasks: "+mIntersectionTasks.size());
		return sb.toString();
	}
}
