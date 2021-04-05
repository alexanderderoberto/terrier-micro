package it.cnr.isti.hpclab.finegrained;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.terrier.structures.Index;
import org.terrier.structures.postings.IterablePosting;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.finegrained.ChunkMatchingAlgorithm;
import it.cnr.isti.hpclab.manager.MatchingEntry;
import it.cnr.isti.hpclab.matching.structures.QueryProperties.RuntimeProperty;
import it.cnr.isti.hpclab.matching.structures.ResultSet;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.matching.structures.WeightingModel;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class ChunkManager {
	protected static final Logger LOGGER = Logger.getLogger(ChunkManager.class);
	protected static boolean IGNORE_LOW_IDF_TERMS = MatchingConfiguration.getBoolean(Property.IGNORE_LOW_IDF_TERMS);
	
	public WeightingModel mWeightingModel = null;
	public int numRequired = 0;
	
	protected Index mIndex = null;
	protected ChunkMatchingAlgorithm mMatchingAlgorithm = null;
	
	/** Used to avoid initializing enums multiple times if the query hasn't changed */
	private int lastQueryId = 0;
	
	public List<MatchingEntry> enums;
	
	public long processedPostings;
	
	public long partiallyProcessedDocuments;
	public long numPivots;

	public ChunkManager(final Index index)
	{
		TinyJProfiler.tic();
		String weightingModelClassName = MatchingConfiguration.get(Property.WEIGHTING_MODEL_CLASSNAME);
		createWeigthingModel(weightingModelClassName);
		
		String matchingAlgorithmClassName =  MatchingConfiguration.get(Property.MATCHING_ALGORITHM_CLASSNAME); 
		createMatchingAlgorithm(matchingAlgorithmClassName);
		
		loadIndex(index);
		TinyJProfiler.toc();
	}

	protected void loadIndex(final Index index) 
	{
		TinyJProfiler.tic();
		if (index == null)
			LOGGER.fatal("Can't create chunk manager with null index");
		mIndex = index;

		mWeightingModel.setup(mIndex);
		mMatchingAlgorithm.setup(this);
		TinyJProfiler.toc();
	}

	protected void createWeigthingModel(final String weightingModelClassName) 
	{
		TinyJProfiler.tic();
		checkNotNull(weightingModelClassName);
		try {
			if (weightingModelClassName.indexOf('.') == -1)
				mWeightingModel = (WeightingModel) (Class.forName(MatchingConfiguration.get(Property.DEFAULT_NAMESPACE) + "structures.model." + weightingModelClassName).asSubclass(WeightingModel.class).getConstructor().newInstance());
			else
				mWeightingModel = (WeightingModel) (Class.forName(weightingModelClassName).asSubclass(WeightingModel.class).getConstructor().newInstance());
		} catch (Exception e) {
			LOGGER.error("Problem loading weighting model (" + weightingModelClassName + "): ", e);
		}
		TinyJProfiler.toc();
	}

	protected void createMatchingAlgorithm(final String matchingAlgorithmClassName) 
	{
		TinyJProfiler.tic();
		checkNotNull(matchingAlgorithmClassName);
		try {
			if (matchingAlgorithmClassName.indexOf('.') == -1)
				mMatchingAlgorithm = (ChunkMatchingAlgorithm) (Class.forName(MatchingConfiguration.get(Property.DEFAULT_NAMESPACE) + matchingAlgorithmClassName).asSubclass(ChunkMatchingAlgorithm.class).getConstructor().newInstance());
			else
				mMatchingAlgorithm = (ChunkMatchingAlgorithm) (Class.forName(matchingAlgorithmClassName).asSubclass(ChunkMatchingAlgorithm.class).getConstructor().newInstance());
		} catch (Exception e) {
			LOGGER.error("Problem loading matching algorithm (" + matchingAlgorithmClassName + "): ", e);
		}
		TinyJProfiler.toc();
	}

	abstract public ResultSet run(final IntersectionTask task) throws IOException;
	
	public final int TOP_K() 
	{ 
		return MatchingConfiguration.getInt(Property.TOP_K); 
	}

	public void reset_to(final int to) throws IOException
	{
		TinyJProfiler.tic();
		for (MatchingEntry t: enums) {
			t.posting.close();
			t.posting = mIndex.getInvertedIndex().getPostings(t.entry);
			t.posting.next();
			t.posting.next(to);
		}
		TinyJProfiler.toc();
	}
	
	protected void open_enums(final IntersectionTask it) throws IOException {
		TinyJProfiler.tic();
		
		if(it.fgsrq.srq.getQueryId() == lastQueryId){
			TinyJProfiler.toc();
			return;
		}
		
		if(lastQueryId != 0){
			close_enums();//TODO: chiudere gli enums prima di terminare il thread
		}
		
		lastQueryId = it.fgsrq.srq.getQueryId();
		enums = new ObjectArrayList<MatchingEntry>();
		
		for (int i=0; i< it.fgsrq.terms.size(); i++) {
			IterablePosting ip = mIndex.getInvertedIndex().getPostings(it.fgsrq.lexicons.get(i));
			ip.next();
			int qtf = it.fgsrq.srq.getQueryTermFrequency(it.fgsrq.terms.get(i));
			enums.add(new MatchingEntry(qtf, it.fgsrq.terms.get(i), ip, it.fgsrq.lexicons.get(i)));
			if (it.fgsrq.terms.get(i).isRequired())
				numRequired++;//COMM: capire dove si usa
		}
		
		Collections.sort(enums, MatchingEntry.SORT_BY_DOCID);
		
		TinyJProfiler.toc();
	}

	protected void close_enums() throws IOException
	{
		TinyJProfiler.tic();
		for (MatchingEntry pair: enums)
        	pair.posting.close();
		TinyJProfiler.toc();
	}
	
	public final int min_docid() 
	{
		TinyJProfiler.tic();
		int docid = Integer.MAX_VALUE;
		for (int i = 0; i < enums.size(); i++)
			if (enums.get(i).posting.getId() < docid)
				docid = enums.get(i).posting.getId();
		TinyJProfiler.toc();
		return docid;
	}
	
	public abstract void stats(final SearchRequest srq) throws IOException;
	
	protected void stats_enums(final SearchRequest srq)
	{
		TinyJProfiler.tic();
		srq.getQuery().addMetadata(RuntimeProperty.QUERY_TERMS, Arrays.toString(enums.stream().map(x -> "\"" + x.term + "\"").collect(Collectors.toList()).toArray()));
		srq.getQuery().addMetadata(RuntimeProperty.QUERY_LENGTH,    Integer.toString(enums.size()));
		srq.getQuery().addMetadata(RuntimeProperty.PROCESSED_TERMS_DF, Arrays.toString(enums.stream().map(x -> x.entry.getDocumentFrequency()).collect(Collectors.toList()).toArray()));
		TinyJProfiler.toc();
	}
}
