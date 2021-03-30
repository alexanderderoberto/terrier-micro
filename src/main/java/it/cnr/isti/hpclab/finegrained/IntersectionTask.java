package it.cnr.isti.hpclab.finegrained;

import java.util.List;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.manager.MatchingEntry;
import it.cnr.isti.hpclab.matching.structures.TopQueue;

public class IntersectionTask {
	private static int staticId = 0;
	private final int id;
	
	public final FineGrainedSearchRequest fgsrq;
	public final int fromDocId;
	public final int toDocId;
	List<MatchingEntry> enums;
	public TopQueue heap;
	
	public long processedPostings;
	
	public IntersectionTask(FineGrainedSearchRequest fgsrq)
	{
		this(fgsrq, null, 0, 0);
	}
	
	public IntersectionTask(FineGrainedSearchRequest fgsrq, List<MatchingEntry> enums, int fromId, int toId)
	{
		this.id = ++staticId;
		this.fgsrq = fgsrq;
		this.enums = enums;
		if(fgsrq != null){
			this.heap = new TopQueue(MatchingConfiguration.getInt(Property.TOP_K), fgsrq.initialThreshold);
		}
		
		fromDocId = fromId;
		toDocId = toId;
		
		processedPostings = 0l;
	}
	
	public boolean isPoison()
	{
		return this.fgsrq == null;
	}
	
	public List<MatchingEntry> getEnums(){
		return this.enums;
	}
	
	public TopQueue getHeap(){
		return this.heap;
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("Task n." + id +", query n."+fgsrq.srq.getQueryId()+" ");
		sb.append(" common span: ("+fromDocId+"-"+fromDocId+")");
		return sb.toString();
	}
}
