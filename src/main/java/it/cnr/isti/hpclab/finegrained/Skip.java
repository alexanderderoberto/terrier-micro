package it.cnr.isti.hpclab.finegrained;

public class Skip {
	public int beginning;
	public int end;
	
	public Skip(int beginning, int end) {
		this.beginning = beginning;
		this.end = end;
	}
	
	public int contains(int value){
		if(value < beginning)
			return -1;
		if(value <= end)
			return 0;
		else
			return 1;
	}
}
