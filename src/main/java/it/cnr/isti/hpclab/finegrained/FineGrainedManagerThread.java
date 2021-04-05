package it.cnr.isti.hpclab.finegrained;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.terrier.structures.Index;
import org.terrier.structures.IndexOnDisk;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.annotations.Managed;
import it.cnr.isti.hpclab.finegrained.ChunkManager;
import it.cnr.isti.hpclab.matching.structures.QueryProperties.RuntimeProperty;
import it.cnr.isti.hpclab.matching.structures.resultset.EmptyResultSet;
import it.cnr.isti.hpclab.matching.structures.resultset.ScoredResultSet;
import it.cnr.isti.hpclab.parallel.SearchRequestMessage;

public class FineGrainedManagerThread extends Thread
{
	protected static final Logger LOGGER = Logger.getLogger(FineGrainedManagerThread.class);
	
	// Private variables
	private final ChunkManager mManager;
	
	// Private variables
	private IndexOnDisk mIndex;

	// Shared variables
	protected final BlockingQueue<IntersectionTask> sIntersectionTaskQueue;
	protected final BlockingQueue<SearchRequestMessage> sResultQueue;
	
	// Static variables
	private static int staticId = 0;
	
	private ChunkManager create_manager()
	{
		TinyJProfiler.tic();
		try {
			mIndex = Index.createIndex();
			String matchingAlgorithmClassName =  MatchingConfiguration.get(Property.MATCHING_ALGORITHM_CLASSNAME);
			if (matchingAlgorithmClassName.indexOf('.') == -1)
				matchingAlgorithmClassName = MatchingConfiguration.get(Property.DEFAULT_NAMESPACE) + matchingAlgorithmClassName;
			String mManagerClassName = Class.forName(matchingAlgorithmClassName).asSubclass(ChunkMatchingAlgorithm.class).getAnnotation(Managed.class).by();
			
			// return (Manager) (Class.forName(mManagerClassName).asSubclass(Manager.class).getConstructor().newInstance(mIndex));
			ChunkManager tictoc = (ChunkManager) Class.forName(mManagerClassName).asSubclass(ChunkManager.class).getConstructor(Index.class).newInstance(mIndex);
			TinyJProfiler.toc();
			return tictoc;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public FineGrainedManagerThread(final BlockingQueue<IntersectionTask> itask_queue, final BlockingQueue<SearchRequestMessage> res_queue)
	{
		TinyJProfiler.tic();
		super.setName(this.getClass().getSimpleName() + "_" + (++staticId));
		LOGGER.warn(super.getName() + " is going to build its own index copy");
		
		// shared
		this.sIntersectionTaskQueue = itask_queue;
		this.sResultQueue = res_queue;
		
		// private
		this.mManager = create_manager();
		TinyJProfiler.toc();
    }
	
	@Override
	public void run() 
	{
		TinyJProfiler.tic();
		try {
			while (true) {
				TinyJProfiler.tic("<idle_thread>");
				IntersectionTask it = sIntersectionTaskQueue.take();
				TinyJProfiler.toc("<idle_thread>");
				if (it.isPoison()) // poison pill received, no more intersection tasks to process
					break;
				
				mManager.run(it);
				
				if(it.fgsrq.isCompleted()){
					//heap = it.fgsrq.merge();
					
					mManager.stats(it.fgsrq.srq);//TODO: sistemare in modo elegante
					addFinalStats(it.fgsrq);
					
					if (it.fgsrq.heap.isEmpty())
						it.fgsrq.srq.setResultSet(new EmptyResultSet());
					else
						it.fgsrq.srq.setResultSet(new ScoredResultSet(it.fgsrq.heap.getQueue()));
						
					sResultQueue.put(new SearchRequestMessage(it.fgsrq.srq));
				}
			}
			mIndex.close();
			// notify I'm done to result writer with a poison pill
			sResultQueue.put(new SearchRequestMessage(null));
		} catch (Exception e) {
			e.printStackTrace();
		}
		LOGGER.info(super.getName() + " terminating...");
		TinyJProfiler.toc();
	}
	
	private void addFinalStats(final FineGrainedSearchRequest fgsrq){
		TinyJProfiler.tic();
        fgsrq.srq.getQuery().addMetadata(RuntimeProperty.FINAL_THRESHOLD,    Float.toString(fgsrq.heap.threshold()));
        fgsrq.srq.getQuery().addMetadata(RuntimeProperty.INITIAL_THRESHOLD,  Float.toString(fgsrq.initialThreshold));
		fgsrq.srq.getQuery().addMetadata(RuntimeProperty.NUM_RESULTS, 	     Integer.toString(fgsrq.heap.size()));
		fgsrq.srq.getQuery().addMetadata(RuntimeProperty.PROCESSED_POSTINGS, Long.toString(fgsrq.processedPostings));
		fgsrq.srq.getQuery().addMetadata(RuntimeProperty.PROCESSING_TIME,    Double.toString(fgsrq.getProcessingTime()/1e6));
		TinyJProfiler.toc();
	}
}
