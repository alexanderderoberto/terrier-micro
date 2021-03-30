package it.cnr.isti.hpclab.finegrained;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.terrier.structures.Index;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.postings.IterablePosting;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.finegrained.ChunkMatchingAlgorithm;
import it.cnr.isti.hpclab.manager.MatchingEntry;
import it.cnr.isti.hpclab.matching.structures.QueryProperties.RuntimeProperty;
import it.cnr.isti.hpclab.matching.structures.ResultSet;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.matching.structures.WeightingModel;
import it.cnr.isti.hpclab.matching.structures.query.QueryTerm;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class ChunkManager {
	protected static final Logger LOGGER = Logger.getLogger(ChunkManager.class);
	protected static boolean IGNORE_LOW_IDF_TERMS = MatchingConfiguration.getBoolean(Property.IGNORE_LOW_IDF_TERMS);
	
	public WeightingModel mWeightingModel = null;
	public int numRequired = 0;
	
	protected Index mIndex = null;
	protected ChunkMatchingAlgorithm mMatchingAlgorithm = null;
	
	
	public List<MatchingEntry> enums;
	
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

	abstract public ResultSet run(final IntersectionTask task) throws IOException;
	
	public final int TOP_K() 
	{ 
		return MatchingConfiguration.getInt(Property.TOP_K); 
	}

	public void reset_to(final int to) throws IOException
	{
		for (MatchingEntry t: enums) {
			t.posting.close();
			t.posting =  mIndex.getInvertedIndex().getPostings(t.entry);
			t.posting.next();
			t.posting.next(to);
		}
	}
	
	protected abstract MatchingEntry entryFrom(final int qtf, final QueryTerm term, final IterablePosting posting, final LexiconEntry entry) throws IOException;

	protected void open_enums(final IntersectionTask it) throws IOException
	{
		/*
		enums = it.getEnums();
		
		if(enums != null)
			return;
		*/
		
		final SearchRequest searchRequest = it.fgsrq.srq;
		
		enums = new ObjectArrayList<MatchingEntry>();
		
		final int num_docs = mIndex.getCollectionStatistics().getNumberOfDocuments();
		
		// We look in the index and filter out common terms
		for (QueryTerm queryTerm: searchRequest.getQueryTerms()) {
			LexiconEntry le = mIndex.getLexicon().getLexiconEntry(queryTerm.getQueryTerm());
			if (le == null) {
				LOGGER.warn("Term not found in index: " + queryTerm.getQueryTerm());
			} else if (IGNORE_LOW_IDF_TERMS && le.getFrequency() > num_docs) {
				LOGGER.warn("Term " + queryTerm.getQueryTerm() + " has low idf - ignored from scoring.");
			} else {
				IterablePosting ip = mIndex.getInvertedIndex().getPostings(le);
				ip.next();
				// enums.add(new MatchingEntry(term, ip, le));
				int qtf = searchRequest.getQueryTermFrequency(queryTerm);
				enums.add(entryFrom(qtf, queryTerm, ip, le));
				if (queryTerm.isRequired())
					numRequired++;
			}
		}
	}

	protected void close_enums() throws IOException
	{
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
	
	public abstract void stats(final SearchRequest srq);
	
	protected void stats_enums(final SearchRequest srq)
	{
		srq.getQuery().addMetadata(RuntimeProperty.QUERY_TERMS, Arrays.toString(enums.stream().map(x -> "\"" + x.term + "\"").collect(Collectors.toList()).toArray()));
		srq.getQuery().addMetadata(RuntimeProperty.QUERY_LENGTH,    Integer.toString(enums.size()));
		srq.getQuery().addMetadata(RuntimeProperty.PROCESSED_TERMS_DF, Arrays.toString(enums.stream().map(x -> x.entry.getDocumentFrequency()).collect(Collectors.toList()).toArray()));        
	}
}
