package it.cnr.isti.hpclab.parallel.finegrained;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.terrier.structures.Index;
import org.terrier.structures.IndexOnDisk;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.postings.IterablePosting;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.matching.structures.query.QueryTerm;
import it.cnr.isti.hpclab.parallel.SearchRequestMessage;

public class QuerySplittingThread extends Thread {
	protected static final Logger LOGGER = Logger.getLogger(QuerySplittingThread.class);
	protected static boolean IGNORE_LOW_IDF_TERMS = MatchingConfiguration.getBoolean(Property.IGNORE_LOW_IDF_TERMS);
	protected static int TASKS_PER_THREAD = MatchingConfiguration.getInt(Property.TASKS_PER_THREAD);
	protected static int TASKS_QUEUE_TRESHOLD = MatchingConfiguration.getInt(Property.TASKS_QUEUE_TRESHOLD);
	
	/** Number of ocuments indexed by this index */
	protected final long numDocsInIndex;
	
	// Shared variables
	protected final IndexOnDisk mIndex;
	protected final BlockingQueue<SearchRequestMessage> sSearchRequestQueue;
	protected final BlockingQueue<IntersectionTask> sIntersectionTaskQueue;
	protected final Object splittersLock;
	
	// private
	private final int mNumComputingThreads;
	
	// Static variables
	private static int staticId = 0;
	private static int splittingThreadsCount = 0;
	
	public QuerySplittingThread(final BlockingQueue<SearchRequestMessage> sreq_queue, final BlockingQueue<IntersectionTask> itask_queue, Object splittersLock, final int mNumComputingThreads)
	{
		super.setName(this.getClass().getSimpleName() + "_" + (++staticId));
		LOGGER.warn(super.getName() + " is going to build its own index copy");
		
		splittingThreadsCount++;
		
		// shared
		this.sSearchRequestQueue = sreq_queue;
		this.sIntersectionTaskQueue = itask_queue;
		this.splittersLock = splittersLock;
		
		// private
		this.mIndex = Index.createIndex();
		this.mNumComputingThreads = mNumComputingThreads;
		
		numDocsInIndex = mIndex.getCollectionStatistics().getNumberOfDocuments();
	}
	
	@Override
	public void run()
	{
		try {
			while (true) {
				SearchRequestMessage m = sSearchRequestQueue.take();
				
				if (m.isPoison()) // poison pill received, no more intersection tasks to process
					break;
				
				LOGGER.info(super.getName() + " processing query " + m.srq.getQueryId() + " : " + m.srq.getOriginalQuery());
				
				// Start measuring query execution time from this point
				long startTime = System.nanoTime();
				
				// Find the term with shortest skip list
				int numberOfPointers = Integer.MAX_VALUE;
				LexiconEntry shortestTermLE = null;
				ArrayList<QueryTerm> importantTerms = new ArrayList<QueryTerm>();
				ArrayList<LexiconEntry> importantLE = new ArrayList<LexiconEntry>();
				for (QueryTerm queryTerm: m.srq.getQueryTerms()) {
					LexiconEntry le = mIndex.getLexicon().getLexiconEntry(queryTerm.getQueryTerm());
					if (le == null) {
						LOGGER.warn("Term not found in index: " + queryTerm.getQueryTerm());
					} else if (IGNORE_LOW_IDF_TERMS && le.getFrequency() > numDocsInIndex) {
						LOGGER.warn("Term " + queryTerm.getQueryTerm() + " has low idf - ignored from scoring.");
					} else {
						importantTerms.add(queryTerm);
						importantLE.add(le);
						int pointers = SkipsReader.numberOfPointers(mIndex, le);
						if(pointers < numberOfPointers){
							numberOfPointers = pointers;
							shortestTermLE = le;
						}
					}
				}
				
				if(shortestTermLE == null)
					break;
				
				// Count how many tasks we need
				int numberOfTasks = mNumComputingThreads * TASKS_PER_THREAD;
				int step = (numberOfPointers + 1) / numberOfTasks;
				if(step > 0){
					int remainder = (numberOfPointers + 1) % numberOfTasks;
					numberOfTasks += remainder / step;
					if((remainder % step) != 0)
						numberOfTasks++;
				}
				else{
					numberOfTasks = numberOfPointers + 1;
					step = 1;
				}
				
				FineGrainedSearchRequest fgsr = new FineGrainedSearchRequest(m.srq, importantTerms, importantLE, numberOfTasks, startTime);
				
				// Get the list of first docIds of each block (or bunch of blocks, depending on step)
				int[] blocks = createSkipList(mIndex, shortestTermLE, numberOfTasks, step);
				
				for(int i=0; i<(numberOfTasks - 1); i++){
					IntersectionTask it = new IntersectionTask(fgsr, blocks[i], blocks[i + 1] - 1);
					fgsr.addIntersectionTask(it);
					sIntersectionTaskQueue.put(it);
				}
				IntersectionTask it = new IntersectionTask(fgsr, blocks[(numberOfTasks - 1)], IterablePosting.END_OF_LIST);
				fgsr.addIntersectionTask(it);
				sIntersectionTaskQueue.put(it);
				
				if(sIntersectionTaskQueue.size() > TASKS_QUEUE_TRESHOLD){
					synchronized(splittersLock){
						splittersLock.wait();
					}
				}
			}
			
			// If this is the last splitting thread to terminate
			if(--splittingThreadsCount == 0){
				// Notify with a poison pill that no more queries are available
				for(int i=0; i<mNumComputingThreads; i++)
					sIntersectionTaskQueue.put(new IntersectionTask(null));
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		LOGGER.info(super.getName() + " terminating...");
	}
	
	private static int[] createSkipList(IndexOnDisk index, LexiconEntry le, int numberOfBlocks, int step) throws IOException
	{
		SkipsReader sr = new SkipsReader(index, le, step);
		
		int[] blocks = new int[numberOfBlocks];
		for(int i=0; i<numberOfBlocks; i++)
			blocks[i] = (int)sr.next();
		
		return blocks;
	}
}