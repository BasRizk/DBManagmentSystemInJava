package teamName;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import teamName.Tuple;

public class Page implements Serializable {

	/**
	 *	Having a fixed serialVersionUID to be able to Deserialize objects on any version of java
	 */
	private static final long serialVersionUID = 1L;
		
	private ArrayList<Tuple> rows;
	private int numOfRows;
	private String pageName;
	
	public Page(String pageName) {
				
		// Supported Types:
		// java.lang.Integer, java.lang.String, java.lang.Double, 
		// java.lang.Boolean and java.util.Date
		
		this.pageName = pageName;
		this.rows = new ArrayList<Tuple>();
		this.numOfRows = rows.size();
						
	}
		
	public void insertRow(Hashtable<String, Object> htblColNameValue) {
		
	    Tuple tuple = new Tuple(htblColNameValue);
	    rows.add(tuple);
	    this.numOfRows++;
	    
	}
			
	public int getNumOfRows() {
		return numOfRows;
	}

	public String getPageName() {
		return pageName;
	}
	
	public void serializePage(String tablePath) {
		
		//File tableDir = new File("../tableName/");
		//tablePath.mkdirs();
		
		try {
			
            FileOutputStream fos = new FileOutputStream(tablePath);
            ObjectOutputStream oos;
            oos = new ObjectOutputStream(fos);
            oos.writeObject(this);
            oos.close();
            fos.close();
	        
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
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
            //TODO Auto-generated catch block
            e.printStackTrace();
        }
		
		return page;
	}

	public void printPage() {
		
		System.out.println(pageName + " : " + numOfRows + " rows: ");
		for(Tuple row : rows) {
			row.printTuple();
		}
		
	}


}