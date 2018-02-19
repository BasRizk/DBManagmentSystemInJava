package teamName;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

public class Table implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 8010842808275391794L;
	
	private String tableName;
	private int size;
	private String primaryKey;
	private Hashtable<String, String> ColName_Type;
	private ArrayList<String> pagePathes;
	private String lastPagePath;
	private int numOfRows;
	private int numOfCol;
	
	
	
	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType) throws DBAppException {
		
		this.tableName = strTableName;
		this.primaryKey = strClusteringKeyColumn;
		
		this.ColName_Type = htblColNameType;
		this.ColName_Type.put("TouchData","java.util.Date");
		
		if(!checkColumns(htblColNameType))
			throw new DBAppException("Unsupported Datatype!");
		
		// Supported Types: java.lang.Integer, java.lang.String, java.lang.Double, 
		// java.lang.Boolean and java.util.Date
		
		
		this.numOfCol = ColName_Type.size();
		this.numOfRows = 0;
		String pageName = this.tableName + "_0001";
		Page page = new Page (pageName);
		
		this.lastPagePath = "../"+strTableName+"/"+pageName + ".ser";
		this.pagePathes.add(this.lastPagePath);
		
		try {
			FileOutputStream fos = new FileOutputStream(this.lastPagePath);
			ObjectOutputStream oos;
			oos = new ObjectOutputStream(fos);
			oos.writeObject(page);
			oos.close();
			fos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		createMetadataFile();
		
	}
	
	
	private boolean checkColumns(Hashtable<String, String> htblColNameType) {
		
		for (String key : htblColNameType.keySet()) {
			if(!(htblColNameType.get(key).equals("java.lang.Integer")
					|| htblColNameType.get(key).equals("java.lang.String") 
					|| htblColNameType.get(key).equals("java.lang.Double")
					|| htblColNameType.get(key).equals("java.lang.Boolean")
					|| htblColNameType.get(key).equals("java.util.Date")) ){
				return false;
			}
		}
		return true;
		
	}
	
	private void createMetadataFile() {
		
		//TODO 9 metadata file for all tables
		
		String fileName = "metadata.csv";
		
		String data = "";
		
		Enumeration<String> colNames = ColName_Type.keys();
		
		while(colNames.hasMoreElements()) {
			
			String colName = colNames.nextElement();
			String colType = ColName_Type.get(colName);
			boolean isKey = colName == primaryKey;
			
			boolean isIndexed = isIndexed(colName);
			
			data = data +  this.tableName + ", " + colName + ", " + colType + ", " +
						isKey + ", " + isIndexed + "\n";
		}
		
		try {
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(fileName)));
			
			bw.write(data);
			bw.close();
		
		} catch (IOException e) {
			
			System.out.println(e);
			
		}

	}
	
	public void insertIntoPage(Hashtable<String,Object> htblColNameValue) throws DBAppException {
	    
	    String pagePath = getLastPagePath();
	    
	    try {
	        
            FileInputStream fis = new FileInputStream(pagePath);
            ObjectInputStream ois;
            ois = new ObjectInputStream(fis);
            Page page = (Page) ois.readObject();
            ois.close();
            fis.close();
            page.insertRow(htblColNameValue);
            
        } catch (IOException e) {
            //TODO Auto-generated catch block
            e.printStackTrace();
        
        } catch (ClassNotFoundException c) {
            
            throw new DBAppException("Page not Found");
        
        }
	    
	}
	
	public boolean isIndexed(String colName) {
		//TODO 11 depends on indexing way later
		return false;
	}
	
	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	public String getName() {
		return tableName;
	}
	public int getSize() {
		return size;
	}
	public ArrayList<String> getPagePathes() {
		return pagePathes;
	}
	public String getLastPagePath() {
		return lastPagePath;
	}
	
	
}
