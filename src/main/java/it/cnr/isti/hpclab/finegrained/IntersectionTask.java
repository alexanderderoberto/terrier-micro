package it.cnr.isti.hpclab.finegrained;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.matching.structures.TopQueue;

public class IntersectionTask {
	public final FineGrainedSearchRequest fgsrq;
	public final int fromDocId;
	public final int toDocId;
	
	public long processedPostings;
	public TopQueue heap;
	
	public IntersectionTask(FineGrainedSearchRequest fgsrq) {
		this(fgsrq, 0, 0);
	}
	
	public IntersectionTask(FineGrainedSearchRequest fgsrq, int fromId, int toId) {
		TinyJProfiler.tic();
		
		this.fgsrq = fgsrq;
		this.fromDocId = fromId;
		this.toDocId = toId;
		this.processedPostings = 0L;
		
		/*
		if(fgsrq != null)
			this.heap = new TopQueue(MatchingConfiguration.getInt(Property.TOP_K), fgsrq.initialThreshold);
		*/
		
		TinyJProfiler.toc();
	}
	
	public boolean isPoison() {
		return this.fgsrq == null;
	}
}
