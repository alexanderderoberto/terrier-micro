package it.cnr.isti.hpclab.finegrained;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.terrier.structures.Index;
import org.terrier.structures.IndexOnDisk;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.postings.IterablePosting;

import it.cnr.isti.hpclab.ef.structures.EFLexiconEntry;
import it.cnr.isti.hpclab.finegrained.BoundedMatchingEntry;
import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.manager.MatchingEntry;
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
	
	protected static int FINE_GRAINED_GRANULARITY = 256;//TODO: spostare in properties
	
	/** The number of matched queries. */
	protected int mMatchingQueryCount = 0;
	
	/** Data structures */
	protected IndexOnDisk  mIndex;
	private QuerySource  mQuerySource;
	
	// to be shared
	protected BlockingQueue<FineGrainedSearchRequest> sSearchRequestQueue;//TODO: controllare se viene utilizzata
	//protected ConcurrentHashMap<Integer, SearchRequest> sPendingSearchRequestMap;//TODO: togliere
	protected BlockingQueue<IntersectionTask> sIntersectionTaskQueue;
	protected BlockingQueue<SearchRequestMessage> sResultQueue;
	
	// private
	private final int mNumThreads;
	private ObjectList<Thread> mThreads;
	
	//DEBUG
	static long timeA = 0;
	static long timeB = 0;
	static long timeC = 0;
	// /DEBUG
	
	public FineGrainedParallelQuerying() 
	{
		mIndex = createIndex();
		
		mQuerySource = createQuerySource();

		sSearchRequestQueue = new ArrayBlockingQueue<FineGrainedSearchRequest>(1 << 10);
		//sPendingSearchRequestMap = new ConcurrentHashMap<Integer, SearchRequest>();//params: query Id, ?
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
					
		LOGGER.info("createSkipList timeA: " + ((timeA) / 1000.0d) + " seconds, timeB: " + ((timeB - timeA) / 1000.0d) + " seconds, timeC: " + ((timeC) / 1000.0d) + " seconds");
	}
	
	public void processQuery(final int queryId, final String query, final float threshold) throws IOException, QueryParserException
	{
		try {
			LOGGER.info("Opening query " + queryId + " : " + query);
			
			FineGrainedSearchRequest fgsr = new FineGrainedSearchRequest(new SearchRequest(queryId, query));
			final int num_docs = mIndex.getCollectionStatistics().getNumberOfDocuments();
			
			//COMM: creo una skipList per ogni termine della query
			int skipNumber = 0;
			
			long startTimeB = System.currentTimeMillis();
			
			for (QueryTerm queryTerm: fgsr.srq.getQueryTerms()) {
				LexiconEntry le = mIndex.getLexicon().getLexiconEntry(queryTerm.getQueryTerm());
				if (le == null) {
					LOGGER.warn("Term not found in index: " + queryTerm.getQueryTerm());
				} else if (IGNORE_LOW_IDF_TERMS && le.getFrequency() > num_docs) {
					LOGGER.warn("Term " + queryTerm.getQueryTerm() + " has low idf - ignored from scoring.");
				} else {
					fgsr.putSkipList(skipNumber, createSkipList(mIndex, queryTerm));
				}
				skipNumber++;
			}
			
			timeB += System.currentTimeMillis() - startTimeB;
			
			fgsr.startTime();
			
			int numRequired = 0;
			List<SkipList> skipLists = fgsr.getSkipLists();
			SkipList shortestList = skipLists.get(0);// Skip list più corta (in quanto sono ordinate per lunghezza durante l'inserimento in fgsrq)
			int taskFrom = 0;
			int taskTo = IterablePosting.END_OF_LIST;
			
			long startTimeC = System.currentTimeMillis();
			
			for(int i=0; i<shortestList.size(); i++){
				Skip ithSkip = shortestList.get(i);// Prendo l'i-esimo skip della lista più corta
				IntersectionTask it = new IntersectionTask(fgsr, null, ithSkip.beginning, ithSkip.end);
				fgsr.addIntersectionTask(it);
				sIntersectionTaskQueue.put(it);
			}
			
			timeC += System.currentTimeMillis() - startTimeC;
			
			sSearchRequestQueue.put(fgsr);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public SkipList createSkipList(Index index, QueryTerm queryTerm) throws IOException {
		long startTime = System.currentTimeMillis();
		
		SkipList skipList = new SkipList(queryTerm);
		
		LexiconEntry le = index.getLexicon().getLexiconEntry(queryTerm.getQueryTerm());
		SkipsReader sr = new SkipsReader((IndexOnDisk)index, le);
		
		long firstDocId = sr.next();
		long lastDocId;
		do{
			lastDocId = sr.next();
			skipList.add(new Skip((int)firstDocId, (int)lastDocId - 1));
		}
		while((firstDocId = lastDocId) != IterablePosting.END_OF_LIST);
		
		timeA += System.currentTimeMillis() - startTime;
		
		return skipList;
	}
}
