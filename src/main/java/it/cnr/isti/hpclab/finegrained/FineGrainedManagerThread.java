package it.cnr.isti.hpclab.finegrained;

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel.MapMode;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.terrier.structures.Index;
import org.terrier.structures.IndexOnDisk;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.postings.IterablePosting;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.annotations.Managed;
import it.cnr.isti.hpclab.ef.EliasFano;
import it.cnr.isti.hpclab.ef.structures.EFLexiconEntry;
import it.cnr.isti.hpclab.ef.util.EFUtils;
import it.cnr.isti.hpclab.ef.util.LongWordBitReader;
import it.cnr.isti.hpclab.finegrained.ChunkManager;
import it.cnr.isti.hpclab.finegrained.ChunkedRankedAnd;
import it.cnr.isti.hpclab.manager.MatchingEntry;
import it.cnr.isti.hpclab.matching.MatchingAlgorithm;
import it.cnr.isti.hpclab.matching.structures.QueryProperties.RuntimeProperty;
import it.cnr.isti.hpclab.matching.structures.ResultSet;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.matching.structures.TopQueue;
import it.cnr.isti.hpclab.matching.structures.query.QueryTerm;
import it.cnr.isti.hpclab.matching.structures.resultset.EmptyResultSet;
import it.cnr.isti.hpclab.matching.structures.resultset.ScoredResultSet;
import it.cnr.isti.hpclab.parallel.SearchRequestMessage;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.util.ByteBufferLongBigList;

public class FineGrainedManagerThread extends Thread
{
	protected static final Logger LOGGER = Logger.getLogger(FineGrainedManagerThread.class);
	
	// Private variables
	private final ChunkManager mManager;
	private TopQueue heap;
	
	// Private variables
	private IndexOnDisk mIndex;

	// Shared variables
	protected final BlockingQueue<IntersectionTask> sIntersectionTaskQueue;
	protected final BlockingQueue<SearchRequestMessage> sResultQueue;
	
	// Static variables
	private static int staticId = 0;
	
	private ChunkManager create_manager()
	{
		try {
			mIndex = Index.createIndex();
			String matchingAlgorithmClassName =  MatchingConfiguration.get(Property.MATCHING_ALGORITHM_CLASSNAME);
			if (matchingAlgorithmClassName.indexOf('.') == -1)
				matchingAlgorithmClassName = MatchingConfiguration.get(Property.DEFAULT_NAMESPACE) + matchingAlgorithmClassName;
			String mManagerClassName = Class.forName(matchingAlgorithmClassName).asSubclass(ChunkMatchingAlgorithm.class).getAnnotation(Managed.class).by();
			
			// return (Manager) (Class.forName(mManagerClassName).asSubclass(Manager.class).getConstructor().newInstance(mIndex));
			return (ChunkManager) Class.forName(mManagerClassName).asSubclass(ChunkManager.class).getConstructor(Index.class).newInstance(mIndex);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public FineGrainedManagerThread(final BlockingQueue<IntersectionTask> itask_queue, final BlockingQueue<SearchRequestMessage> res_queue)
	{
		super.setName(this.getClass().getSimpleName() + "_" + (++staticId));
		LOGGER.warn(super.getName() + " is going to build its own index copy");
		
		// shared
		this.sIntersectionTaskQueue = itask_queue;
		this.sResultQueue = res_queue;
		
		// private
		this.mManager = create_manager();
    }
	
	@Override
	public void run() 
	{
		try {
			while (true) {
				IntersectionTask it = sIntersectionTaskQueue.take();
				if (it.isPoison()) // poison pill received, no more intersection tasks to process
					break;
				
				//LOGGER.info(super.getName() + " is processing " + it.toString());
				mManager.run(it);
				
				if(it.fgsrq.isCompleted()){
					heap = it.fgsrq.merge();
					
					mManager.stats(it.fgsrq.srq);//TODO: sistemare in modo elegante
					addFinalStats(it.fgsrq);
					
					if (heap.isEmpty())
						it.fgsrq.srq.setResultSet(new EmptyResultSet());
					else
						it.fgsrq.srq.setResultSet(new ScoredResultSet(heap));
						
					sResultQueue.put(new SearchRequestMessage(it.fgsrq.srq));
				}
			}
			// mManager.close();
			mIndex.close();
			// notify I'm done to result writer with a poison pill
			sResultQueue.put(new SearchRequestMessage(null));
		} catch (Exception e) {
			e.printStackTrace();
		}
		LOGGER.info(super.getName() + " terminating...");
	}
	
	private void addFinalStats(final FineGrainedSearchRequest fgsrq){
        fgsrq.srq.getQuery().addMetadata(RuntimeProperty.FINAL_THRESHOLD,    Float.toString(heap.threshold()));
        fgsrq.srq.getQuery().addMetadata(RuntimeProperty.INITIAL_THRESHOLD,  Float.toString(fgsrq.initialThreshold));
		fgsrq.srq.getQuery().addMetadata(RuntimeProperty.NUM_RESULTS, 	     Integer.toString(heap.size()));
		fgsrq.srq.getQuery().addMetadata(RuntimeProperty.PROCESSED_POSTINGS, Long.toString(fgsrq.processedPostings));
		fgsrq.srq.getQuery().addMetadata(RuntimeProperty.PROCESSING_TIME,    Double.toString(fgsrq.getProcessingTime()/1e6));
	}
}
