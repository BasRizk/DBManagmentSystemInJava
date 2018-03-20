package team_55;

import java.io.Serializable;
import java.util.Hashtable;

public class Tuple implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Hashtable<String, Object> colNameValue;
	
	public Hashtable<String, Object> getColNameValue() {
		return colNameValue;
	}



	public void setColNameValue(Hashtable<String, Object> colNameValue) {
		this.colNameValue = colNameValue;
	}



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