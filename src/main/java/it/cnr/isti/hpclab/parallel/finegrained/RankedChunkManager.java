package it.cnr.isti.hpclab.parallel.finegrained;

import java.io.IOException;

import org.terrier.structures.Index;

import it.cnr.isti.hpclab.matching.structures.ResultSet;
import it.cnr.isti.hpclab.matching.structures.TopQueue;
import it.cnr.isti.hpclab.matching.structures.resultset.EmptyResultSet;

public class RankedChunkManager extends ChunkManager{
	public TopQueue heap;

	public RankedChunkManager(Index index) {
		super(index);
	}

	@Override
	public void run(final IntersectionTask task) throws IOException
	{
		open_enums(task);
		
		if (enums.size() == 0)
			return;
	
		processedPostings = 0L;
		heap = task.heap;
		
		mMatchingAlgorithm.match(task.fromDocId, task.toDocId);

		task.processedPostings = processedPostings;
	}
}
