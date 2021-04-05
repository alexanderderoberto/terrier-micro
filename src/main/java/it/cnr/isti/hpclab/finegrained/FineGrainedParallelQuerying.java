package it.cnr.isti.hpclab.finegrained;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.terrier.structures.Index;
import org.terrier.structures.IndexOnDisk;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.postings.IterablePosting;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.matching.structures.query.QueryParserException;
import it.cnr.isti.hpclab.matching.structures.query.QuerySource;
import it.cnr.isti.hpclab.matching.structures.query.QueryTerm;
import it.cnr.isti.hpclab.matching.structures.query.ThresholdQuerySource;
import it.cnr.isti.hpclab.parallel.ResultOutputThread;
import it.cnr.isti.hpclab.parallel.SearchRequestMessage;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;

public class FineGrainedParallelQuerying
{
	private static final Logger LOGGER = Logger.getLogger(FineGrainedParallelQuerying.class);
	protected static boolean IGNORE_LOW_IDF_TERMS = MatchingConfiguration.getBoolean(Property.IGNORE_LOW_IDF_TERMS);
	
	protected static int TASKS_PER_THREAD = 4;//TODO: spostare in properties
	/** Documents indexed by this index */
	protected final long numDocsInIndex;
	
	/** The number of matched queries. */
	protected int mMatchingQueryCount = 0;//TODO: aggiustare
	
	/** Data structures */
	protected IndexOnDisk  mIndex;
	private QuerySource  mQuerySource;
	
	// to be shared
	protected BlockingQueue<IntersectionTask> sIntersectionTaskQueue;
	protected BlockingQueue<SearchRequestMessage> sResultQueue;
	
	// private
	private final int mNumThreads;
	private ObjectList<Thread> mThreads;
	
	public FineGrainedParallelQuerying(){
		TinyJProfiler.tic();
		
		mIndex = createIndex();
		numDocsInIndex = mIndex.getCollectionStatistics().getNumberOfDocuments();
		
		mQuerySource = createQuerySource();
		
		sIntersectionTaskQueue = new ArrayBlockingQueue<IntersectionTask>(1 << 10);
		sResultQueue = new ArrayBlockingQueue<SearchRequestMessage>(1 << 10);

		mNumThreads = Runtime.getRuntime().availableProcessors();
		mThreads = new ObjectArrayList<Thread>(mNumThreads + 1);
		
		Thread th;
		for (int i = 0; i < mNumThreads; i++) {
			th = new FineGrainedManagerThread(sIntersectionTaskQueue, sResultQueue);
			mThreads.add(th);
			th.start();
		}
		th = new ResultOutputThread(sResultQueue, mNumThreads);
		mThreads.add(th);
		th.start();
		
		TinyJProfiler.toc();
	}
	
	public static IndexOnDisk createIndex(){
		return Index.createIndex();
	}
	
	private static QuerySource createQuerySource() 
	{
		TinyJProfiler.tic();
		try {
			String querySourceClassName =  MatchingConfiguration.get(Property.QUERY_SOURCE_CLASSNAME);
			if (querySourceClassName.indexOf('.') == -1)
				querySourceClassName = MatchingConfiguration.get(Property.DEFAULT_NAMESPACE) + querySourceClassName;
			QuerySource tictoc = (QuerySource) (Class.forName(querySourceClassName).asSubclass(QuerySource.class).getConstructor().newInstance());
			TinyJProfiler.toc();
			return tictoc;
		} catch (Exception e) {
			e.printStackTrace();
			TinyJProfiler.toc();
			return null;
		}
	}
	
	public void processQueries() throws IOException, QueryParserException {
		TinyJProfiler.tic();
		
		mQuerySource.reset();

		final long startTime = System.currentTimeMillis();

		// iterating through the queries
		while (mQuerySource.hasNext()) {
			String query = mQuerySource.next();
			int qid   = mQuerySource.getQueryId();
			
			float  qth   = 0.0f;
			if (mQuerySource instanceof ThresholdQuerySource)
				qth = ((ThresholdQuerySource) mQuerySource).getQueryThreshold();
			
			processQuery(qid, query, qth);
		}
		
		// notify processors that queries are over with a poison pill per processor
		try {
			for (int i = 0; i < mNumThreads; ++i) {
				sIntersectionTaskQueue.put(new IntersectionTask(null));
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		mThreads.forEach(t -> {	try { t.join();	} catch (InterruptedException e) { e.printStackTrace(); } } );
		
		LOGGER.info("Finished topics, executed " + mMatchingQueryCount +
					" queries in " + ((System.currentTimeMillis() - startTime) / 1000.0d) +
					" seconds");
					
		TinyJProfiler.toc();
		TinyJProfiler.close();
	}
	
	public void processQuery(final int queryId, final String query, final float threshold) throws IOException, QueryParserException {
		TinyJProfiler.tic();
		try {
			LOGGER.info("Opening query " + queryId + " : " + query);
			
			// Find the term with shortest skip list
			SearchRequest srq = new SearchRequest(queryId, query);
			int numberOfPointers = Integer.MAX_VALUE;
			LexiconEntry shortestTermLE = null;
			ArrayList<QueryTerm> importantTerms = new ArrayList<QueryTerm>();
			ArrayList<LexiconEntry> importantLE = new ArrayList<LexiconEntry>();
			for (QueryTerm queryTerm: srq.getQueryTerms()) {
				LexiconEntry le = mIndex.getLexicon().getLexiconEntry(queryTerm.getQueryTerm());
				if (le == null) {
					LOGGER.warn("Term not found in index: " + queryTerm.getQueryTerm());
				} else if (IGNORE_LOW_IDF_TERMS && le.getFrequency() > numDocsInIndex) {
					LOGGER.warn("Term " + queryTerm.getQueryTerm() + " has low idf - ignored from scoring.");
				} else {
					importantTerms.add(queryTerm);
					importantLE.add(le);
					int pointers = SkipsReader.numberOfPointers((IndexOnDisk)mIndex, le);
					if(pointers < numberOfPointers){
						numberOfPointers = pointers;
						shortestTermLE = le;
					}
				}
			}
			
			if(shortestTermLE == null)
				return;
			
			// Count how many tasks we need
			int numberOfTasks = mNumThreads * TASKS_PER_THREAD;
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
			
			FineGrainedSearchRequest fgsr = new FineGrainedSearchRequest(srq, importantTerms, importantLE, numberOfTasks);
			
			// Get the list of first docIds of each skip
			int[] blocks = createSkipList(mIndex, shortestTermLE, numberOfTasks, step);
			
			for(int i=0; i<(numberOfTasks - 1); i++){
				IntersectionTask it = new IntersectionTask(fgsr, blocks[i], blocks[i + 1] - 1);
				fgsr.addIntersectionTask(it);
				sIntersectionTaskQueue.put(it);
			}
			IntersectionTask it = new IntersectionTask(fgsr, blocks[(numberOfTasks - 1)], IterablePosting.END_OF_LIST);
			fgsr.addIntersectionTask(it);
			sIntersectionTaskQueue.put(it);
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		TinyJProfiler.toc();
	}
	
	public int[] createSkipList(Index index, LexiconEntry le, int numberOfBlocks, int step) throws IOException {
		TinyJProfiler.tic();
		
		SkipsReader sr = new SkipsReader((IndexOnDisk)index, le, step);
		
		int[] blocks = new int[numberOfBlocks];
		for(int i=0; i<numberOfBlocks; i++)
			blocks[i] = (int)sr.next();
			
		TinyJProfiler.toc();
		return blocks;
	}
}
