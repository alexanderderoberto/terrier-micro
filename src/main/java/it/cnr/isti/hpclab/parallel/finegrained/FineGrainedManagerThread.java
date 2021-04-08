package it.cnr.isti.hpclab.parallel.finegrained;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.terrier.structures.Index;
import org.terrier.structures.IndexOnDisk;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.annotations.Managed;
import it.cnr.isti.hpclab.parallel.finegrained.ChunkManager;
import it.cnr.isti.hpclab.matching.structures.TopQueue;
import it.cnr.isti.hpclab.matching.structures.resultset.EmptyResultSet;
import it.cnr.isti.hpclab.matching.structures.resultset.ScoredResultSet;
import it.cnr.isti.hpclab.parallel.SearchRequestMessage;

public class FineGrainedManagerThread extends Thread
{
	protected static final Logger LOGGER = Logger.getLogger(FineGrainedManagerThread.class);
	protected static int TASKS_QUEUE_TRESHOLD = MatchingConfiguration.getInt(Property.TASKS_QUEUE_TRESHOLD);
	
	// Private variables
	private IndexOnDisk mIndex;
	private final ChunkManager mManager;

	// Shared variables
	protected final BlockingQueue<IntersectionTask> sIntersectionTaskQueue;
	protected final BlockingQueue<SearchRequestMessage> sResultQueue;
	protected final Object splittersLock;
	
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
			
			return (ChunkManager) Class.forName(mManagerClassName).asSubclass(ChunkManager.class).getConstructor(Index.class).newInstance(mIndex);
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public FineGrainedManagerThread(final BlockingQueue<IntersectionTask> itask_queue, final BlockingQueue<SearchRequestMessage> res_queue, Object splittersLock)
	{
		super.setName(this.getClass().getSimpleName() + "_" + (++staticId));
		LOGGER.warn(super.getName() + " is going to build its own index copy");
		
		// shared
		this.sIntersectionTaskQueue = itask_queue;
		this.sResultQueue = res_queue;
		this.splittersLock = splittersLock;
		
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
				
				mManager.run(it);
				
				if(it.fgsrq.isCompleted()){
					TopQueue heap = it.fgsrq.merge();
					
					it.fgsrq.addStatistics(mManager.enums, heap);
					
					if (heap.isEmpty())
						it.fgsrq.srq.setResultSet(new EmptyResultSet());
					else
						it.fgsrq.srq.setResultSet(new ScoredResultSet(heap));
						
					sResultQueue.put(new SearchRequestMessage(it.fgsrq.srq));
					
					if(sIntersectionTaskQueue.size() <= TASKS_QUEUE_TRESHOLD){
						synchronized(splittersLock){
							splittersLock.notify();
						}
					}
				}
			}
			mManager.close();
			mIndex.close();
			// notify I'm done to result writer with a poison pill
			sResultQueue.put(new SearchRequestMessage(null));
		} catch (Exception e) {
			e.printStackTrace();
		}
		LOGGER.info(super.getName() + " terminating...");
	}
}
