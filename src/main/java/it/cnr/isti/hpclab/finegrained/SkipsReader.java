package it.cnr.isti.hpclab.finegrained;

import it.cnr.isti.hpclab.ef.EliasFano;
import it.cnr.isti.hpclab.ef.structures.EFLexiconEntry;
import it.cnr.isti.hpclab.ef.util.EFUtils;
import it.cnr.isti.hpclab.ef.util.LongWordBitReader;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.util.ByteBufferLongBigList;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel.MapMode;
import org.terrier.structures.IndexOnDisk;
import org.terrier.structures.Pointer;
import org.terrier.structures.postings.IterablePosting;

public class SkipsReader{
	/** Log2 of the number of zeros each "upper bits" portion contains */
	protected final int log2Quantum;
	/** Content of the index, encoded with Elias Fano*/
	protected final LongBigList docidsList;
	/** Data structure which extracts the pointers to the skips */
	protected final LongWordBitReader skipPointers;
	/** The bit offset of the portion containing the array of skip pointers in the docids file, it is also the initial bit offset of this posting list in the index */
	protected final long skipPointersStart;
	/** The size in bits of the pointers to the skips */
	protected final int pointerSize;
	/** The number of skip pointers */
	protected final long numberOfPointers;
	/** Data structure which extracts "lower bits" */
	protected final LongWordBitReader lowerBits;
	/** The bit offset of the portion containing the array og "lower bits" in the docids file */
	protected final long lowerBitsStart;
	/** The size in bits of "lower bits" portion in the actual encoding */
	protected final int lbSize;
	/** The bit offset of the portion containing the array of "upper bits" in the docids file */
	protected final long upperBitsStart;
	/** The block of "upper bits" I'm currently using. The first block (n. 0) has no skip pointers pointing to it, it starts at upperBitsStart. The first skip pointer points to the block n. 1 */
	private int block;
	/** The index of the current prefix sum. */
	private long currentDocIdIndex;
	
	
	public SkipsReader(final IndexOnDisk index, final Pointer pointer) throws IOException{
		TinyJProfiler.tic();
		/** The number of documents that this entry occurs in */
		final int numDocuments = ((EFLexiconEntry)pointer).getDocumentFrequency();
		/** Upper bound on the number of documents */
		final int upperBoundDocid = index.getCollectionStatistics().getNumberOfDocuments();
		
		pointerSize = EFUtils.pointerSize(numDocuments + 1, upperBoundDocid, false, true);
		lbSize = EFUtils.lowerBits(numDocuments + 1, upperBoundDocid, false);
		log2Quantum = index.getIntIndexProperty(EliasFano.LOG2QUANTUM, 0);
		numberOfPointers = EFUtils.numberOfPointers(numDocuments + 1, upperBoundDocid, log2Quantum, false, true);
		
		skipPointersStart = ((EFLexiconEntry)pointer).getDocidOffset();
		lowerBitsStart = skipPointersStart + pointerSize * numberOfPointers;
		upperBitsStart = lowerBitsStart + lbSize * (numDocuments + 1L);
		
		String byteOrderString = index.getIndexProperty(EliasFano.BYTEORDER, "");
        ByteOrder byteOrder;
        if ("LITTLE_ENDIAN".equals(byteOrderString)) {
            byteOrder = ByteOrder.LITTLE_ENDIAN;
        } else if ("BIG_ENDIAN".equals(byteOrderString)) {
            byteOrder = ByteOrder.BIG_ENDIAN;
        } else {
            throw new IllegalStateException();
        }
		
		FileInputStream fileInputStream = new FileInputStream( index.getPath() + File.separator + index.getPrefix() + EliasFano.DOCID_EXTENSION );
		docidsList = ByteBufferLongBigList.map( fileInputStream.getChannel(), byteOrder, MapMode.READ_ONLY );
		fileInputStream.close();
		
		skipPointers = new LongWordBitReader(docidsList, pointerSize);
		lowerBits = new LongWordBitReader(docidsList, lbSize);
		TinyJProfiler.toc();
	}
	
	private long getNextUpperBits(final long position){
		/** The 64-bit buffer used to read the upper bits */
		long window;
		/** The pointer to the word (i.e. Long) in the docidsList. Used to read the upper bits. */
		long curr;
		
		TinyJProfiler.tic();
		window = docidsList.getLong(curr = position / Long.SIZE) & -1L << (int)(position);//COMM: a cosa serve questo shift?
		while (window == 0)
			window = docidsList.getLong( ++curr );
		
		long lastUpperBits = curr * Long.SIZE + Long.numberOfTrailingZeros(window) - currentDocIdIndex++ - upperBitsStart;
		window &= window - 1;
		TinyJProfiler.toc();
		return lastUpperBits;
	}
	
	/** Returns the first docId in the block being currently used. Increments the actual block at every call */
	public long next(){
		TinyJProfiler.tic();
		if(block > numberOfPointers){
			TinyJProfiler.toc();
			return IterablePosting.END_OF_LIST;
		}
			
		final long precedingZeroes = block << log2Quantum; //precedingZeroes: number of zeros that preceding blocks must contain (i.e. decompressed value of the first element in the actual block)
		
		if(block++ == 0){
			long tictoc = getNextUpperBits(upperBitsStart) << lbSize | lowerBits.extract();
			TinyJProfiler.toc();
			return tictoc;
		}
		else{
			final long skip = skipPointers.extract(skipPointersStart + (block - 2) * pointerSize); //skip: bit offset of the beginning of the "block"-th block w.r.t upperBitsStart
			assert skip != 0;
			currentDocIdIndex = skip - precedingZeroes;//currentDocIdIndex: the number of documents indexed by all the preceding blocks
			long tictoc = getNextUpperBits(upperBitsStart + skip) << lbSize | lowerBits.extract(lowerBitsStart + lbSize * currentDocIdIndex);
			TinyJProfiler.toc();
			return tictoc;
		}
	}
	
	/** Returns the number of skip pointers for the term represented by the passed "Pointer" */
	public static int numberOfPointers(final IndexOnDisk index, final Pointer pointer){
		TinyJProfiler.tic();
		int tictoc = (int)EFUtils.numberOfPointers(((EFLexiconEntry)pointer).getDocumentFrequency() + 1, index.getCollectionStatistics().getNumberOfDocuments(), index.getIntIndexProperty(EliasFano.LOG2QUANTUM, 0), false, true);
		TinyJProfiler.toc();
		return tictoc;
	}
}