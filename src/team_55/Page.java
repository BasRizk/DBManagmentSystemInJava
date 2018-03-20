package team_55;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;


public class Page implements Serializable {

	private static final long serialVersionUID = 1L;
		
	private ArrayList<Tuple> rows;
	private int numOfRows;
	private String pageName;
	private boolean deleted;
	
	public Page(String pageName) {
		
		this.pageName = pageName;
		this.rows = new ArrayList<Tuple>();
		this.numOfRows = rows.size();
						
	}
	
	public void deleteRow(Hashtable<String, Object> htblColNameValue, String primaryKey){
		//TODO 
		//deleted = false;
		//boolean found = true;
		
	    Tuple reqTuple = null;
	    
		for (Tuple tuple : rows) {
						
		    if(htblColNameValue.get(primaryKey).equals(tuple.getColNameValue().get(primaryKey))) {
		        reqTuple = tuple;
		        break;
		    }
		    
		}
		
		if(reqTuple != null) {
		    rows.remove(rows.indexOf(reqTuple));
		    numOfRows--;
		} else {
		    System.out.println("Tuple does not exist");
		}
		
	}
	
	public void insertRow(Hashtable<String, Object> htblColNameValue) {
		
	    Tuple tuple = new Tuple(htblColNameValue);
	    rows.add(tuple);
	    this.numOfRows++;
	    
	}
			
	private Tuple findTupleString(String strKey , String primaryKeyName) {
		Tuple reqTuple = null;
		for (Tuple tuple : rows) {
			if(tuple.getColNameValue().get(primaryKeyName).equals(strKey)) {
				reqTuple = tuple;
				break;
			}
				
		}
		return reqTuple;
	}

    private Tuple findTupleInt(int intKey, String primaryKeyName) {
        Tuple reqTuple = null;
        for (Tuple tuple : rows) {
            if (tuple.getColNameValue().get(primaryKeyName).equals(intKey)) {
                reqTuple = tuple;
                break;
            }

        }
        return reqTuple;
    }
    
    private Tuple findTupleDouble(Double doubleKey, String primaryKeyName) {
        Tuple reqTuple = null;
        for (Tuple tuple : rows) {
            if (tuple.getColNameValue().get(primaryKeyName).equals(doubleKey)) {
                reqTuple = tuple;
                break;
            }

        }
        return reqTuple;
    }
    
    private Tuple findTupleDate(Date dateKey, String primaryKeyName) {
        Tuple reqTuple = null;
        for (Tuple tuple : rows) {
            if (tuple.getColNameValue().get(primaryKeyName).equals(dateKey)) {
                reqTuple = tuple;
                break;
            }

        }
        return reqTuple;
    }
	
	public void updateRow(String strKey, String primaryKeyName ,Hashtable<String, Object> htblColNameValue, String primaryKeyType) {
	    
	    Tuple tuple = null;
	    
	    if(primaryKeyType.equals("java.lang.Integer")) {
	        int intKey = Integer.parseInt(strKey);
	        tuple = findTupleInt(intKey, primaryKeyName);
	    } else if(primaryKeyType.equals("java.lang.Double")) {
	        Double doubleKey = Double.parseDouble(strKey);
            tuple = findTupleDouble(doubleKey, primaryKeyName);
	    } else if(primaryKeyType.equals("java.lang.Date")) {
	        Date dateKey = (Date) ((Object) strKey);
            tuple = findTupleDate(dateKey, primaryKeyName);
	    } else {
	        tuple = findTupleString(strKey, primaryKeyName);
	    }
	    
		if(tuple != null) {
			for (String key : htblColNameValue.keySet()) {  //updating each item in the hashtable of the tuple according to the provided hashtable
				tuple.getColNameValue().remove(key);
				tuple.getColNameValue().put(key, htblColNameValue.get(key));
			}
			
			tuple.getColNameValue().remove("TouchDate");
			tuple.getColNameValue().put("TouchDate", new Date());	
		}
	
					
	}
	
	public void serializePage(String tablePath) {
		
		try {
			
            FileOutputStream fos = new FileOutputStream(tablePath);
            ObjectOutputStream oos;
            oos = new ObjectOutputStream(fos);
            oos.writeObject(this);
            oos.close();
            fos.close();
	        
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	public static Page deserializePage(String pagePath) {
		
		Page page = null;
		
		try {
	        
            FileInputStream fis = new FileInputStream(pagePath);
            ObjectInputStream ois;
            ois = new ObjectInputStream(fis);
            page = (Page) ois.readObject();
            ois.close();
            fis.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
		
		return page;
	}
	
	public int getNumOfRows() {
		return numOfRows;
	}

	public String getPageName() {
		return pageName;
	}
	
	public ArrayList<Tuple> getRows() {
		return rows;
	}

	public boolean isDeleted() {
		return deleted;
	}
	
	public void printPage() {
		
		System.out.println("---Page--- " + pageName + " : " + numOfRows + " rows: ");
		for(Tuple row : rows) {
			row.printTuple();
		}
		
	}

}