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

public class WorkerThread extends Thread {
	protected static final Logger LOGGER = Logger.getLogger(WorkerThread.class);
	//protected static int TASKS_QUEUE_TRESHOLD = MatchingConfiguration.getInt(Property.TASKS_QUEUE_TRESHOLD);
	
	// Private variables
	private IndexOnDisk mIndex;
	private final ChunkManager mManager;

	// Shared variables
	//protected final BlockingQueue<IntersectionTask> sIntersectionTaskQueue;
	protected final ResourceManager resourceManager;
	protected final BlockingQueue<SearchRequestMessage> sResultQueue;
	//protected final Object splittersLock;
	
	// Static variables
	private static int staticId = 0;
	
	public final int id;
	
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
	
	public WorkerThread(final ResourceManager rm, final BlockingQueue<SearchRequestMessage> res_queue)
	{
		super.setName(this.getClass().getSimpleName() + "_" + (++staticId));
		LOGGER.warn(super.getName() + " is going to build its own index copy");
		
		// shared
		this.resourceManager = rm;
		this.sResultQueue = res_queue;
		
		// private
		this.mManager = create_manager();
		
		id = staticId;
    }
	
	@Override
	public void run() 
	{
		try {
			while (true) {
				IntersectionTask it = resourceManager.takeTask(this);
				
				if(it == null) { // No more tasks in this queue, unbind
					resourceManager.unbindThreadFromQueue(this);
					continue;
				}
				
				if (it.isPoison()) {// poison pill received, no more intersection tasks to process
					resourceManager.unbindThreadFromQueue(this);
					System.out.println("Final unbinding thread " + this.getName());
					//resourceManager.print();
					break;
				}
				
				mManager.run(it);
				
				if(it.fgsrq.isCompleted(it)){
					it.fgsrq.addStatistics(mManager.enums);
					
					if (it.fgsrq.heap.isEmpty())
						it.fgsrq.srq.setResultSet(new EmptyResultSet());
					else
						it.fgsrq.srq.setResultSet(new ScoredResultSet(it.fgsrq.heap));
						
					sResultQueue.put(new SearchRequestMessage(it.fgsrq.srq));
				}
			}
			
			// Thread termination
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
