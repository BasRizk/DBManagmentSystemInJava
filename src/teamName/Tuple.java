package teamName;

import java.io.Serializable;
import java.util.Hashtable;

public class Tuple implements Serializable{

	Hashtable<String, Object> colNameValue;
	
	public Tuple(Hashtable<String, Object> htblColNameValue) {
		
		this.colNameValue = htblColNameValue;
		
	}
	
	public void printTuple() {
		
		for(String key : colNameValue.keySet()) {
			System.out.print(colNameValue.get(key).toString() + " : ");
		}
		System.out.println();
		
	}
}