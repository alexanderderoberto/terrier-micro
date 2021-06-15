package it.cnr.isti.hpclab.parallel.finegrained;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.terrier.structures.Index;
import org.terrier.structures.postings.IterablePosting;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.parallel.finegrained.ChunkMatchingAlgorithm;
import it.cnr.isti.hpclab.manager.MatchingEntry;
import it.cnr.isti.hpclab.matching.structures.ResultSet;
import it.cnr.isti.hpclab.matching.structures.WeightingModel;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class ChunkManager {
	protected static final Logger LOGGER = Logger.getLogger(ChunkManager.class);
	
	protected Index mIndex = null;
	public WeightingModel mWeightingModel = null;
	protected ChunkMatchingAlgorithm mMatchingAlgorithm = null;
	
	/** Used to avoid initializing enums multiple times if the query hasn't changed */
	private int lastQueryId = 0;
	public List<MatchingEntry> enums;
	
	public int numRequired = 0;
	public long processedPostings;
	public long partiallyProcessedDocuments;
	public long numPivots;

	public ChunkManager(final Index index)
	{
		String weightingModelClassName = MatchingConfiguration.get(Property.WEIGHTING_MODEL_CLASSNAME);
		createWeigthingModel(weightingModelClassName);
		
		String matchingAlgorithmClassName =  MatchingConfiguration.get(Property.MATCHING_ALGORITHM_CLASSNAME); 
		createMatchingAlgorithm(matchingAlgorithmClassName);
		
		loadIndex(index);
	}

	protected void loadIndex(final Index index) 
	{
		if (index == null)
			LOGGER.fatal("Can't create chunk manager with null index");
		mIndex = index;

		mWeightingModel.setup(mIndex);
		mMatchingAlgorithm.setup(this);
	}

	protected void createWeigthingModel(final String weightingModelClassName) 
	{
		checkNotNull(weightingModelClassName);
		try {
			if (weightingModelClassName.indexOf('.') == -1)
				mWeightingModel = (WeightingModel) (Class.forName(MatchingConfiguration.get(Property.DEFAULT_NAMESPACE) + "structures.model." + weightingModelClassName).asSubclass(WeightingModel.class).getConstructor().newInstance());
			else
				mWeightingModel = (WeightingModel) (Class.forName(weightingModelClassName).asSubclass(WeightingModel.class).getConstructor().newInstance());
		} catch (Exception e) {
			LOGGER.error("Problem loading weighting model (" + weightingModelClassName + "): ", e);
		}
	}

	protected void createMatchingAlgorithm(final String matchingAlgorithmClassName) 
	{
		checkNotNull(matchingAlgorithmClassName);
		try {
			if (matchingAlgorithmClassName.indexOf('.') == -1)
				mMatchingAlgorithm = (ChunkMatchingAlgorithm) (Class.forName(MatchingConfiguration.get(Property.DEFAULT_NAMESPACE) + matchingAlgorithmClassName).asSubclass(ChunkMatchingAlgorithm.class).getConstructor().newInstance());
			else
				mMatchingAlgorithm = (ChunkMatchingAlgorithm) (Class.forName(matchingAlgorithmClassName).asSubclass(ChunkMatchingAlgorithm.class).getConstructor().newInstance());
		} catch (Exception e) {
			LOGGER.error("Problem loading matching algorithm (" + matchingAlgorithmClassName + "): ", e);
		}
	}

	abstract public void run(final IntersectionTask task) throws IOException;
	
	public final int TOP_K() 
	{ 
		return MatchingConfiguration.getInt(Property.TOP_K); 
	}
	
	protected void open_enums(final IntersectionTask it) throws IOException {
		if(it.fgsrq.srq.getQueryId() == lastQueryId)
			return;
		
		if(lastQueryId != 0){
			close_enums();
		}
		
		lastQueryId = it.fgsrq.srq.getQueryId();
		enums = new ObjectArrayList<MatchingEntry>();
		
		for (int i=0; i< it.fgsrq.terms.size(); i++) {
			IterablePosting ip = mIndex.getInvertedIndex().getPostings(it.fgsrq.lexicons.get(i));
			ip.next();
			int qtf = it.fgsrq.srq.getQueryTermFrequency(it.fgsrq.terms.get(i));
			enums.add(new MatchingEntry(qtf, it.fgsrq.terms.get(i), ip, it.fgsrq.lexicons.get(i)));
			if (it.fgsrq.terms.get(i).isRequired())
				numRequired++;
		}
		
		Collections.sort(enums, MatchingEntry.SORT_BY_DOCID);
	}
	
	public void close() throws IOException
	{
		close_enums();
	}

	protected void close_enums() throws IOException
	{
		if(enums == null)
			return;
		
		for (MatchingEntry pair: enums)
        	pair.posting.close();
	}
	
	public final int min_docid() 
	{
		int docid = Integer.MAX_VALUE;
		for (int i = 0; i < enums.size(); i++)
			if (enums.get(i).posting.getId() < docid)
				docid = enums.get(i).posting.getId();
		return docid;
	}
}
