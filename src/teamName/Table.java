package teamName;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
	private static final long serialVersionUID = 1L;
	
	private String tableName;
	private String primaryKey;
	private Hashtable<String, String> ColName_Type;
	private ArrayList<String> pagePathes;
	private ArrayList<String> freePagesPathes; // contains free pages path does NOT contain lastPagePath
	private String lastPagePath;
	private int numOfRows;
	private int numOfCol;
	private int maxPageRowNumber; //the maximum number of rows in 1 page for now 200;
	
	
	
	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType , int maxPageRowNumber) throws DBAppException {
		
		pagePathes = new ArrayList<String>();
		freePagesPathes = new ArrayList<String>();
		
		this.tableName = strTableName;
		this.primaryKey = strClusteringKeyColumn;
		
		this.ColName_Type = htblColNameType;
		this.ColName_Type.put("TouchData","java.util.Date");
		
		if(!checkColumns(htblColNameType))
			throw new DBAppException("Unsupported Datatype!");
				
		this.numOfCol = ColName_Type.size();
		this.numOfRows = 0;

		File pagesDir = new File("TablePages/"+strTableName);
		pagesDir.mkdirs();
		
		
		// Creating first page using the method
		createPage();
		
		/*
		String pageName = this.tableName + "_1";
		Page page = new Page (pageName);
		
		this.lastPagePath = "TablePages/" + strTableName + "/" + pageName + ".page";
		this.pagePathes.add(this.lastPagePath);
		
		try {
			FileOutputStream fos = new FileOutputStream(this.lastPagePath);
			ObjectOutputStream oos;
			oos = new ObjectOutputStream(fos);
			oos.writeObject(page);
			oos.close();
			fos.close();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		*/
		
		//Create meta-data file only on the first page
		if(this.pagePathes.size() == 1) {
			appendToMetadataFile();
		}
		serializeTable();		
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
	
	private void appendToMetadataFile() {
				
		String filePath = "data\\metadata.csv";
		
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
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(filePath, true));
			
			bw.write(data);
			bw.close();
		
		} catch (IOException e) {
			
			System.out.println(e);
			
		}
		
	}
	
	private void createPage() {
	    

		// This was here before like that (Forget TablePages folder)
        // this.lastPagePath = "../"+tableName+"/" + pageName + ".page";        
        
		// You forgot to add up pagePathes.size() + 1 first :D
		
	    String pageName = this.tableName + "_" + (pagePathes.size() + 1);
        Page page = new Page (pageName);
       
		//this.lastPagePath = "../TablePages/" + tableName + "/" + pageName + ".page";
		this.lastPagePath = "TablePages/" + tableName + "/" + pageName + ".page";
        this.pagePathes.add(this.lastPagePath);
        
        // Serialize the page you just created
        page.serializePage(this.lastPagePath);
        
        /*
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
        */
        
        serializeTable();
	    
	}
	public void deleteFromPage(Hashtable<String,Object> htblColNameValue) throws DBAppException {
		//TODO Delete certain record from page
		Page page = null;
		for (String path : pagePathes) {
			page = Page.deserializePage(path);
			page.deleteRow(htblColNameValue);
			page.serializePage(path);
			if(page.isDeleted() && !(freePagesPathes.contains(path))){//check if recoreds had been deleted from the page to put it in freepage array
				freePagesPathes.add(path);			
				}
		}
		serializeTable();
		
	}
	
	public void insertIntoPage(Hashtable<String,Object> htblColNameValue) throws DBAppException {
		
		String pagePath;
		
		if(this.freePagesPathes.size() == 0)     //here we check if there is another page free other than the last one
			pagePath = getLastPagePath();
		else {
			pagePath = freePagesPathes.get(0);   //if we found a page we use it 
		}
	    
		Page page = Page.deserializePage(pagePath);
        page.insertRow(htblColNameValue);

        if(page.getNumOfRows() == maxPageRowNumber) {
        	if(pagePath.equals(lastPagePath))
        		createPage();
        	else
        		freePagesPathes.remove(pagePath);
        }
		
        page.serializePage(pagePath);
      
        /*
		try {
	        
            FileInputStream fis = new FileInputStream(pagePath);
            ObjectInputStream ois;
            ois = new ObjectInputStream(fis);
            Page page = (Page) ois.readObject();
            ois.close();
            fis.close();
            page.insertRow(htblColNameValue);
            
            if(page.getNumOfRows() == maxPageRowNumber) {
            	if(pagePath.equals(lastPagePath))
            		createPage();
            	else
            		freePagesPathes.remove(pagePath);
            }
            
            
        } catch (IOException e) {
            //TODO Auto-generated catch block
            e.printStackTrace();
        
        } catch (ClassNotFoundException c) {
            
            throw new DBAppException("Page not Found");
        
        }
        */
	    numOfRows++;
	    serializeTable();	    
	}
	
	private void serializeTable() {
		
		String tablePath = "Tables/"+tableName+".table";
		File tableDir = new File("Tables/");
		tableDir.mkdirs();
		
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
	
	public static Table deserializeTable(String tablePath) {
		
		Table table = null;
		
		try {
	        
            FileInputStream fis = new FileInputStream(tablePath);
            ObjectInputStream ois = new ObjectInputStream(fis);
            Object object  = ois.readObject();

            table = (Table) object;
            ois.close();
            fis.close();
            
        } catch (Exception e) {
            //TODO Auto-generated catch block
            e.printStackTrace();
        }
		
		return table;
	}
	
	public void printTableData() {
		
		System.out.println(tableName + " : " + numOfRows + " total num of rows: " + "\n");
	
		for(String key : ColName_Type.keySet()) {
			System.out.print(ColName_Type.get(key).toString() + " : ");
		}
		
		for(String path : pagePathes) {
			System.out.println("\n");
			Page page = Page.deserializePage(path);
			page.printPage();
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

	public ArrayList<String> getPagePathes() {
		return pagePathes;
	}
	
	public String getLastPagePath() {
		return lastPagePath;
	}

	public int getNumOfRows() {
		return numOfRows;
	}

	public int getNumOfCol() {
		return numOfCol;
	}
	
	
	
	
}
