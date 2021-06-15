package it.cnr.isti.hpclab.parallel.finegrained;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
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
import it.cnr.isti.hpclab.matching.structures.QueryProperties.RuntimeProperty;
import it.cnr.isti.hpclab.matching.structures.query.QueryTerm;

public class ProducerThread extends Thread {
	protected static final Logger LOGGER = Logger.getLogger(ProducerThread.class);
	protected static boolean IGNORE_LOW_IDF_TERMS = MatchingConfiguration.getBoolean(Property.IGNORE_LOW_IDF_TERMS);
	protected static int TASKS_PER_THREAD = MatchingConfiguration.getInt(Property.TASKS_PER_THREAD);
	protected static String EXECUTION_TIMES_MATRIX = MatchingConfiguration.get(Property.EXECUTION_TIMES_MATRIX);
	
	/** Number of ocuments indexed by this index */
	protected final long numDocsInIndex;
	
	// Shared variables
	protected final IndexOnDisk mIndex;
	protected final BlockingQueue<TimedSearchRequestMessage> sSearchRequestQueue;
	protected final ResourceManager resourceManager;
	
	// private
	private int mNumComputingThreads;
	private int[][] executionTimesMatrix;
	
	// Static variables
	private static int staticId = 0;
	
	public ProducerThread(final BlockingQueue<TimedSearchRequestMessage> sreq_queue, final ResourceManager rm, final int mNumComputingThreads)
	{
		super.setName(this.getClass().getSimpleName() + "_" + (++staticId));
		LOGGER.warn(super.getName() + " is going to build its own index copy");
		
		// shared
		this.sSearchRequestQueue = sreq_queue;
		this.resourceManager = rm;
		
		// private
		this.mIndex = Index.createIndex();
		this.mNumComputingThreads = mNumComputingThreads;
		
		numDocsInIndex = mIndex.getCollectionStatistics().getNumberOfDocuments();
		
		loadExecutionTimesMatrix();
	}
	
	@Override
	public void run()
	{
		try {
			while (true) {
				
				TimedSearchRequestMessage m = (TimedSearchRequestMessage)sSearchRequestQueue.take();
				
				if (m.isPoison()) // poison pill received, no more intersection tasks to process
					break;
				
				LOGGER.info(super.getName() + " processing query " + m.srq.getQueryId() + " : " + m.srq.getOriginalQuery());
				
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
				
				// Get the optimal parallelism degree
				int numAssignedThreads = 1;
				if(numberOfPointers > 0)
					numAssignedThreads = getParallelismDegree(shortestTermLE.getDocumentFrequency(), mNumComputingThreads, sSearchRequestQueue.size());
				
				// Count how many tasks we need
				int step;
				int numberOfTasks = numAssignedThreads * TASKS_PER_THREAD;
				
				if((numberOfPointers + 1) >= numberOfTasks){
					step = (numberOfPointers + 1) / numberOfTasks;
					if(((numberOfPointers + 1) % numberOfTasks) != 0)
						step++;
				}
				else if((numberOfPointers + 1) >= numAssignedThreads){
					step = (numberOfPointers + 1) / numAssignedThreads;
					if(((numberOfPointers + 1) % numAssignedThreads) != 0)
						step++;
				}
				else{
					numberOfTasks = numberOfPointers + 1;
					step = 1;
				}
				
				if(numAssignedThreads > numberOfTasks)
					numAssignedThreads = numberOfTasks;
				
				FineGrainedSearchRequest fgsr = new FineGrainedSearchRequest(m.srq, importantTerms, importantLE, numberOfTasks, m.startTime);
				
				// Get the list of first docIds of each block (or bunch of blocks, depending on step)
				int[] blocks = createSkipList(mIndex, shortestTermLE, numberOfTasks, step);
				
				// Create intersection tasks
				IntersectionTask queue[] = new IntersectionTask[numberOfTasks];
				for(int i=0; i<(numberOfTasks - 1); i++){
					queue[i] = new IntersectionTask(fgsr, blocks[i], blocks[i + 1] - 1);
					fgsr.addIntersectionTask(queue[i]);
				}
				queue[numberOfTasks - 1] = new IntersectionTask(fgsr, blocks[(numberOfTasks - 1)], IterablePosting.END_OF_LIST);
				fgsr.addIntersectionTask(queue[numberOfTasks - 1]);
				
				// Assign newly created intersection tasks to a queue
				int queueId = resourceManager.bindITQueue(queue, numAssignedThreads, m.srq.getQueryId());
				
				// Bind threads to the newly created queue
				while(numAssignedThreads > 0) {
					resourceManager.bindThreadToQueue(queueId);
					numAssignedThreads--;
				}
				
			}
			
			// Notify worker threads with a poison pill that no more queries are available
			while(mNumComputingThreads > 0) {
				IntersectionTask queue[] = new IntersectionTask[1];
				queue[0] = new IntersectionTask(null);
				int queueId = resourceManager.bindITQueue(queue, 1, -1);
				resourceManager.bindThreadToQueue(queueId);
				mNumComputingThreads--;
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
	
	private int getParallelismDegree(int documentFrequency, int numThreads, int queueLength) {
		// Get the row index
		int rowIndex = 1;
		for(int i=2; i<executionTimesMatrix.length - 1; i++) {
			if(documentFrequency <= executionTimesMatrix[i][0]) {
				rowIndex = i;
				break;
			}
		}
		
		float minTime = Float.MAX_VALUE;
		int columnIndex = 1;
		int maxColumns = Math.min(executionTimesMatrix[0].length, numThreads + 1);
		for(int i=columnIndex; i<maxColumns; i++) {
			float value = executionTimesMatrix[rowIndex][i] + executionTimesMatrix[rowIndex][i] * queueLength * i / (float)numThreads;
			if(value < minTime) {
				minTime = value;
				columnIndex = i;
			}
		}
		return executionTimesMatrix[0][columnIndex];
	}
	
	private void loadExecutionTimesMatrix() {
		try {
			int lines = countLines(EXECUTION_TIMES_MATRIX);
			executionTimesMatrix = new int[lines][];
			FileReader fr = new FileReader(EXECUTION_TIMES_MATRIX);
			BufferedReader br = new BufferedReader(fr);
		    String line;
		    int lineCounter = 0;
		    while ((line = br.readLine()) != null) {
		        String[] valuesStrings = line.split(",");
		        int[] values = new int[valuesStrings.length];
		        for(int i=0; i<valuesStrings.length; i++) {
		        	values[i] = Integer.parseInt(valuesStrings[i]);
		        }
		        executionTimesMatrix[lineCounter++] = values;
		    }
		    br.close();
		    fr.close();
		} catch(IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	// Taken from: https://stackoverflow.com/questions/453018/number-of-lines-in-a-file-in-java
	public static int countLines(String filename) throws IOException {
		InputStream is = new BufferedInputStream(new FileInputStream(filename));
		try {
			byte[] c = new byte[1024];

			int readChars = is.read(c);
			if (readChars == -1) {
				// bail out if nothing to read
				return 0;
			}

			// make it easy for the optimizer to tune this loop
			int count = 0;
			while (readChars == 1024) {
				for (int i=0; i<1024;) {
					if (c[i++] == '\n') {
						++count;
					}
				}
				readChars = is.read(c);
			}

			// count remaining characters
			while (readChars != -1) {
				System.out.println(readChars);
				for (int i=0; i<readChars; ++i) {
					if (c[i] == '\n') {
						++count;
					}
				}
				readChars = is.read(c);
			}

			return count == 0 ? 1 : count;
		} finally {
			is.close();
		}
	}
	
}