package it.cnr.isti.hpclab.finegrained;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
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
	
	/** Documents indexed by this index */
	protected final long numDocsInIndex;
	
	/** The number of matched queries. */
	protected int mMatchingQueryCount = 0;//TODO: aggiustare
	
	/** Data structures */
	protected IndexOnDisk  mIndex;
	private QuerySource  mQuerySource;
	
	// to be shared
	protected BlockingQueue<SearchRequestMessage> sSearchRequestQueue;
	protected BlockingQueue<IntersectionTask> sIntersectionTaskQueue;
	protected BlockingQueue<SearchRequestMessage> sResultQueue;
	
	// private
	private final int mNumSplittingThreads;
	private final int mNumComputingThreads;
	private ObjectList<Thread> mThreads;
	
	public FineGrainedParallelQuerying(){
		TinyJProfiler.tic();
		
		mIndex = createIndex();
		numDocsInIndex = mIndex.getCollectionStatistics().getNumberOfDocuments();
		
		mQuerySource = createQuerySource();
		
		sSearchRequestQueue = new ArrayBlockingQueue<SearchRequestMessage>(1 << 10);
		sIntersectionTaskQueue = new ArrayBlockingQueue<IntersectionTask>(1 << 10);
		sResultQueue = new ArrayBlockingQueue<SearchRequestMessage>(1 << 10);

		mNumSplittingThreads = Runtime.getRuntime().availableProcessors();
		mNumComputingThreads = Runtime.getRuntime().availableProcessors();
		mThreads = new ObjectArrayList<Thread>(mNumSplittingThreads + mNumComputingThreads + 1);
		
		Thread th;
		// Create some threads to split incoming queries
		for (int i = 0; i < mNumSplittingThreads; i++) {
			th = new QuerySplittingThread(mIndex, sSearchRequestQueue, sIntersectionTaskQueue, mNumComputingThreads);
			mThreads.add(th);
			th.start();
		}
		
		// Create some threads to process previously generated splits
		for (int i = 0; i < mNumComputingThreads; i++) {
			th = new FineGrainedManagerThread(sIntersectionTaskQueue, sResultQueue);
			mThreads.add(th);
			th.start();
		}
		
		// Create a thread to output the result
		th = new ResultOutputThread(sResultQueue, mNumComputingThreads);
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
			for (int i = 0; i < mNumSplittingThreads; ++i) {
				sSearchRequestQueue.put(new SearchRequestMessage(null));
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
			sSearchRequestQueue.put(new SearchRequestMessage(new SearchRequest(queryId, query)));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		TinyJProfiler.toc();
	}
}
