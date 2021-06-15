package it.cnr.isti.hpclab.parallel.finegrained;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.terrier.structures.Index;
import org.terrier.structures.IndexOnDisk;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.matching.structures.query.QueryParserException;
import it.cnr.isti.hpclab.matching.structures.query.QuerySource;
import it.cnr.isti.hpclab.matching.structures.query.ThresholdQuerySource;
import it.cnr.isti.hpclab.parallel.ResultOutputThread;
import it.cnr.isti.hpclab.parallel.SearchRequestMessage;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;

public class FineGrainedParallelQuerying
{
	private static final Logger LOGGER = Logger.getLogger(FineGrainedParallelQuerying.class);
	protected static boolean IGNORE_LOW_IDF_TERMS = MatchingConfiguration.getBoolean(Property.IGNORE_LOW_IDF_TERMS);
	protected static int NUM_THREADS = MatchingConfiguration.getInt(Property.NUM_THREADS);
	protected static int QUERIES_PER_SECOND = MatchingConfiguration.getInt(Property.QUERIES_PER_SECOND);
	
	/** The number of matched queries. */
	protected int mMatchingQueryCount = 0;
	
	/** Data structures */
	protected IndexOnDisk  mIndex;
	private QuerySource  mQuerySource;
	protected ResourceManager resourceManager;
	private RateLimiter mRateLimiter;
	
	// to be shared
	protected BlockingQueue<TimedSearchRequestMessage> sSearchRequestQueue;
	protected BlockingQueue<SearchRequestMessage> sResultQueue;
	
	// private
	private final int mNumComputingThreads;
	private ObjectList<Thread> mThreads;
	
	public FineGrainedParallelQuerying()
	{
		mIndex = createIndex();
		mQuerySource = createQuerySource();
		mRateLimiter = new RateLimiter(QUERIES_PER_SECOND);
		
		sSearchRequestQueue = new ArrayBlockingQueue<TimedSearchRequestMessage>(1 << 10);
		//sIntersectionTaskQueue = new LinkedBlockingQueue<IntersectionTask>(1 << 10);
		sResultQueue = new ArrayBlockingQueue<SearchRequestMessage>(1 << 10);
		//splittersLock = new Object();
		
		//mNumSplittingThreads = 1;
		
		// Initialise workers threads
		if(NUM_THREADS > 0)
			mNumComputingThreads = NUM_THREADS;
		else
			mNumComputingThreads = Runtime.getRuntime().availableProcessors();
			
		mThreads = new ObjectArrayList<Thread>(mNumComputingThreads + 2);//+1 querySplittingThread, +1 resultOutputThread
		
		resourceManager = new ResourceManager(mNumComputingThreads);
		
		// Create a thread to split incoming queries
		Thread th1 = new ProducerThread(sSearchRequestQueue, resourceManager, mNumComputingThreads);
		mThreads.add(th1);
		
		Thread th;
		// Create some threads to process previously generated splits
		for (int i = 0; i < mNumComputingThreads; i++) {
			th = new WorkerThread(resourceManager, sResultQueue);
			resourceManager.addWorkerThread(th);
			mThreads.add(th);
			th.start();
		}

		th1.start();//temporaneamente qui
		
		// Create a thread to output the result
		th = new ResultOutputThread(sResultQueue, mNumComputingThreads);
		mThreads.add(th);
		th.start();
		
	}
	
	public static IndexOnDisk createIndex()
	{
		return Index.createIndex();
	}
	
	private static QuerySource createQuerySource() 
	{
		try {
			String querySourceClassName =  MatchingConfiguration.get(Property.QUERY_SOURCE_CLASSNAME);
			if (querySourceClassName.indexOf('.') == -1)
				querySourceClassName = MatchingConfiguration.get(Property.DEFAULT_NAMESPACE) + querySourceClassName;
			return (QuerySource) (Class.forName(querySourceClassName).asSubclass(QuerySource.class).getConstructor().newInstance());
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public void processQueries() throws IOException, QueryParserException
	{
		mQuerySource.reset();

		final long startTime = System.currentTimeMillis();
		
		try {
			// iterating through the queries
			while (mQuerySource.hasNext()) {
				mRateLimiter.await();
				
				String query = mQuerySource.next();
				int qid   = mQuerySource.getQueryId();
				
				float  qth   = 0.0f;
				if (mQuerySource instanceof ThresholdQuerySource)
					qth = ((ThresholdQuerySource) mQuerySource).getQueryThreshold();
				
				processQuery(qid, query, qth);
				mMatchingQueryCount++;
			}
			
			// notify splittingThread that queries are over with a poison pill
			sSearchRequestQueue.put(new TimedSearchRequestMessage(null));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		mThreads.forEach(t -> {	try { t.join();	} catch (InterruptedException e) { e.printStackTrace(); } } );
		
		LOGGER.info("Finished topics, executed " + mMatchingQueryCount +
					" queries in " + ((System.currentTimeMillis() - startTime) / 1000.0d) +
					" seconds");
					
	}
	
	public void processQuery(final int queryId, final String query, final float threshold) throws IOException, QueryParserException
	{
		try {
			sSearchRequestQueue.put(new TimedSearchRequestMessage(new SearchRequest(queryId, query)));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
