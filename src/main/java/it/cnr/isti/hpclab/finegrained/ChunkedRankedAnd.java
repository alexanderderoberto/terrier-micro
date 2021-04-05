package it.cnr.isti.hpclab.finegrained;

import it.cnr.isti.hpclab.annotations.Managed;
import it.cnr.isti.hpclab.finegrained.ChunkManager;
import it.cnr.isti.hpclab.finegrained.RankedChunkManager;
import it.cnr.isti.hpclab.manager.MatchingEntry;
import it.cnr.isti.hpclab.matching.structures.Result;
import it.cnr.isti.hpclab.matching.structures.WeightingModel;

import java.io.IOException;
import java.util.List;

@Managed(by = "it.cnr.isti.hpclab.finegrained.RankedChunkManager")
public class ChunkedRankedAnd implements ChunkMatchingAlgorithm{
	private RankedChunkManager manager;
	
	@Override
	public void setup(final ChunkManager manager) 
	{		
		this.manager = (RankedChunkManager) manager;
	}
	
	@Override
	public long match(int from, int to) throws IOException 
	{
		TinyJProfiler.tic();
		final List<MatchingEntry> enums = manager.enums;
		final ConcurrentTopQueue heap = manager.heap;
		final WeightingModel wm = manager.mWeightingModel;
		
		//manager.reset_to(from);
		enums.get(0).posting.next(from);
		
		long start_time = System.nanoTime();
        int currentDocid = enums.get(0).posting.getId();
        float currentScore = 0.0f;
        int i = 1;
        while (currentDocid < to) {
        	for (; i < enums.size(); i++) {
        		enums.get(i).posting.next(currentDocid);
        		if (enums.get(i).posting.getId() != currentDocid) {
        			currentDocid = enums.get(i).posting.getId();
        			i = 0;
        			break;
        		}
        	}
        	
        	if (i == enums.size()) {
        		currentScore = 0.0f;
        		for (int j = 0; j < enums.size(); j++) {
        			currentScore += wm.score(enums.get(j).qtf, enums.get(j).posting, enums.get(j).entry) * enums.get(j).weight;
        		}
        		manager.processedPostings += enums.size();
        		heap.insert(new Result(currentDocid, currentScore));

        		currentDocid = enums.get(0).posting.next();
        		i = 1;
        	}
        }
		TinyJProfiler.toc();
        return System.nanoTime() - start_time;
	}
}
