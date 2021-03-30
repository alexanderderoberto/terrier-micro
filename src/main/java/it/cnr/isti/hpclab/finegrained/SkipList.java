package it.cnr.isti.hpclab.finegrained;

import java.util.ArrayList;
import java.util.List;

import org.terrier.structures.postings.IterablePosting;

import it.cnr.isti.hpclab.matching.structures.query.QueryTerm;

public class SkipList {
	private List<Skip> skips;
	private final QueryTerm term;
	
	private Skip lastSkipCheckedForBounds;
	
	public SkipList(QueryTerm term) {
		this.term = term;
		skips = new ArrayList();
	}
	
	public void add(Skip s) {
		skips.add(s);
	}
	
	public int size() {
		return skips.size();
	}
	
	public List<Skip> getSkips() {
		return skips;
	}
	
	public Skip get(int index) {
		return skips.get(index);
	}
	
	public QueryTerm getQueryTerm() {
		return term;
	}
	
	public int getLowerBound(int value){
		if(skips.get(0).contains(value) < 0)
			return skips.get(0).beginning;
		if(skips.get(skips.size() - 1).contains(value) > 0)
			return IterablePosting.END_OF_LIST;
		
		int first = 0;
		int last = skips.size() - 1;
		int pointer = 0;
		int lastAnalizedBeginning = skips.get(skips.size() - 1).beginning;
		while(first <= last){
			pointer = (first + last) / 2;
			if(skips.get(pointer).contains(value) == 0)//TODO: possibile leggera ottimizzazione per il caso <, ==, >
				return skips.get(pointer).beginning;
			if(skips.get(pointer).contains(value) > 0){
				first = pointer + 1;
			}
			else{
				last = pointer - 1;
				lastAnalizedBeginning = skips.get(pointer).beginning;
			}
		}
		if(skips.get(pointer).contains(value) == 0)
			return skips.get(pointer).beginning;
		else
			return lastAnalizedBeginning;
	}
	
	public int getUpperBound(int value){
		if(skips.get(0).contains(value) < 0)
			return 0;
		if(skips.get(skips.size() - 1).contains(value) > 0)
			return skips.get(skips.size() - 1).end;
		
		int first = 0;
		int last = skips.size() - 1;
		int pointer = 0;
		int lastAnalizedEnd = skips.get(0).end;
		while(first <= last){
			pointer = (first + last) / 2;
			if(skips.get(pointer).contains(value) == 0)//TODO: possibile leggera ottimizzazione per il caso <, ==, >
				return skips.get(pointer).end;
			if(skips.get(pointer).contains(value) > 0){
				first = pointer + 1;
				lastAnalizedEnd = skips.get(pointer).end;
			}
			else{
				last = pointer - 1;
			}
		}
		if(skips.get(pointer).contains(value) == 0)
			return skips.get(pointer).end;
		else
			return lastAnalizedEnd;
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		for(int i=0;i<skips.size();i++) {
			sb.append("("+skips.get(i).beginning+"-"+skips.get(i).end+") ");
		}
		return sb.toString();
	}
}
