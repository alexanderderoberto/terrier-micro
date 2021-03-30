package it.cnr.isti.hpclab.finegrained;

import java.io.IOException;
import java.util.Collections;
import java.util.NoSuchElementException;

import org.terrier.structures.Index;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.postings.IterablePosting;

import it.cnr.isti.hpclab.manager.MatchingEntry;
import it.cnr.isti.hpclab.matching.structures.QueryProperties.RuntimeProperty;
import it.cnr.isti.hpclab.matching.structures.ResultSet;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.matching.structures.TopQueue;
import it.cnr.isti.hpclab.matching.structures.query.QueryTerm;
import it.cnr.isti.hpclab.matching.structures.resultset.EmptyResultSet;
import it.cnr.isti.hpclab.matching.structures.resultset.ScoredResultSet;

public class RankedChunkManager extends ChunkManager{
	public TopQueue heap;

	public RankedChunkManager(Index index) {
		super(index);
	}

	@Override
	public ResultSet run(final IntersectionTask task) throws IOException
	{
		open_enums(task);
		
		if (enums.size() == 0)
			return new EmptyResultSet();
		
		Collections.sort(enums, MatchingEntry.SORT_BY_DOCID);
	
		processedPostings = 0l;
		
		heap = task.getHeap();
		
		mMatchingAlgorithm.match(task.fromDocId, task.toDocId);

		close_enums();
		
		task.processedPostings = processedPostings;
		
		return new EmptyResultSet();//TODO: non deve restituire niente perchè sarebbe il risultato di uno solo dei frammenti
	}
	
	public void stats(final SearchRequest srq){
		stats_enums(srq);
	}
	
	/*
	@Override
	protected void stats_enums(final SearchRequest srq)
	{
		super.stats_enums(srq);
		
        //srq.getQuery().addMetadata(RuntimeProperty.INITIAL_THRESHOLD,  Float.toString(threshold));
	}
	*/

	@Override
	protected MatchingEntry entryFrom(int qtf, QueryTerm term, IterablePosting posting, LexiconEntry entry) throws IOException {
		return new MatchingEntry(qtf, term, posting, entry);
	}

}
