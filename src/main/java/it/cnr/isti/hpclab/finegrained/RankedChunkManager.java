package it.cnr.isti.hpclab.finegrained;

import java.io.IOException;
import java.util.Collections;

import org.terrier.structures.Index;

import it.cnr.isti.hpclab.manager.MatchingEntry;
import it.cnr.isti.hpclab.matching.structures.ResultSet;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.matching.structures.TopQueue;
import it.cnr.isti.hpclab.matching.structures.resultset.EmptyResultSet;

public class RankedChunkManager extends ChunkManager{
	public TopQueue heap;

	public RankedChunkManager(Index index) {
		super(index);
	}

	@Override
	public ResultSet run(final IntersectionTask task) throws IOException
	{
		TinyJProfiler.tic();
		open_enums(task);
		
		if (enums.size() == 0){
			TinyJProfiler.toc();
			return new EmptyResultSet();
		}
		
		Collections.sort(enums, MatchingEntry.SORT_BY_DOCID);
	
		processedPostings = 0L;
		
		heap = task.heap;
		
		mMatchingAlgorithm.match(task.fromDocId, task.toDocId);

		close_enums();
		
		task.processedPostings = processedPostings;
		
		TinyJProfiler.toc();
		return new EmptyResultSet();//TODO: non deve restituire niente perchè sarebbe il risultato di uno solo dei frammenti
	}
	
	public void stats(final SearchRequest srq){
		TinyJProfiler.tic();
		stats_enums(srq);
		TinyJProfiler.toc();
	}
}
