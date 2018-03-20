package team_55;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

public class Table implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private String tableName;
	private String primaryKeyName;
	private Hashtable<String, String> ColName_Type;
    private Hashtable<String, Boolean> ColName_Indexed;
	private ArrayList<String> pagePathes;
	private ArrayList<String> freePagesPathes; // contains free pages path does NOT contain lastPagePath
	private String lastPagePath;
	private int numOfRows = 0;
	private int numOfCol;
	private int maxPageRowNumber; //the maximum number of rows in 1 page for now 200;
	private ArrayList<Object> primaryKeys;
	private final static String META_DATA_DIR = "data\\metadata.csv";
	
	
	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType , int maxPageRowNumber) throws DBAppException {
		
		pagePathes = new ArrayList<String>();
		freePagesPathes = new ArrayList<String>();
		primaryKeys = new ArrayList<Object>();

		this.tableName = strTableName;
		this.primaryKeyName = strClusteringKeyColumn;
		
		this.maxPageRowNumber = maxPageRowNumber;
		
		this.ColName_Type = htblColNameType;
		this.ColName_Type.put("TouchData","java.util.Date");
		
		if(!checkColumns(htblColNameType))
			throw new DBAppException("Unsupported Datatype!");
				
		this.numOfCol = ColName_Type.size();
		this.numOfRows = 0;
		
		
		@SuppressWarnings("unchecked")
        ArrayList<String> keySet = (ArrayList<String>) ColName_Type.keySet();
        ArrayList<String> keys = keySet;
        for(String key: keys){
            this.ColName_Indexed.put(key, false);
        }

		File pagesDir = new File("TablePages/"+strTableName);
		pagesDir.mkdirs();
		
		
		// Creating first page using the method
		createPage();
		
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
	
	/**
	 * Appends the meta-data of the call on table in the meta-data file.
	 */
	
	private void appendToMetadataFile() {
				
		String filePath = META_DATA_DIR;
		
		String data = "";
		
		Enumeration<String> colNames = ColName_Type.keys();
		
		while(colNames.hasMoreElements()) {
			
			String colName = colNames.nextElement();
			String colType = ColName_Type.get(colName);
			boolean isKey = colName == primaryKeyName;
			
			data = data +  this.tableName + ", " + colName + ", " + colType + ", " +
						isKey + ", " + false + "\n";
		}
		
		try {
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(filePath, true));
			
			bw.write(data);
			bw.close();
		
		} catch (IOException e) {
			
			e.printStackTrace();
			
		}
		
	}
	
	/**
	 * 
	 * @param strColumnName, column name of the table to be changed.
	 * @param indexed, new Value of index column for the correspondance index.
	 */
	public void updateMetaWithIndexedColumn(String strColumnName, boolean indexed) {
		
		String filePath = META_DATA_DIR;
				
		try {
			FileReader metaReader = new FileReader(new File(META_DATA_DIR));
			BufferedReader metaBuffer = new BufferedReader(metaReader);
			String allLines = "";
		
			String line = null;
			while((line = metaBuffer.readLine()) != null) {
				String[] lineSplit = line.split(",");
				String currentTable = lineSplit[0].replaceAll(" ", "");
				String currentColumn = lineSplit[1].replaceAll(" ", "");
				if(currentTable.equals(this.tableName)
						&& currentColumn.equals(strColumnName)) {
					
					line = lineSplit[0] + ", " + lineSplit[1] + ", " + lineSplit[2] + ", " +
							lineSplit[3] + ", " + indexed;
				}
				allLines = allLines + line + "\n";
				
			}
			metaBuffer.close();
					
			BufferedWriter bw = new BufferedWriter(new FileWriter(filePath, true));
			
			bw.write(allLines);
			bw.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	private void createPage() {
	     
        
		// You forgot to add up pagePathes.size() + 1 first :D
		
	    String pageName = this.tableName + "_" + (pagePathes.size() + 1);
        Page page = new Page (pageName);
       
		//this.lastPagePath = "../TablePages/" + tableName + "/" + pageName + ".page";
		this.lastPagePath = "TablePages/" + tableName + "/" + pageName + ".page";
        this.pagePathes.add(this.lastPagePath);
        
        // Serialize the page you just created
        page.serializePage(this.lastPagePath);
        
        serializeTable();
	    
	}
	
	public void deleteFromPage(Hashtable<String,Object> htblColNameValue) throws DBAppException {

		Page page = null;
		
		Object key = primaryKeyExists(htblColNameValue.get(primaryKeyName));
		
		if(key != null)
			primaryKeys.remove(key);
		else
			throw new DBAppException("This key does not exist");
		
		for (String path : pagePathes) {
			
			page = Page.deserializePage(path);

			page.deleteRow(htblColNameValue, this.primaryKeyName);

			page.serializePage(path);
			
			if(page.isDeleted() && !(freePagesPathes.contains(path))) {   //check if records had been deleted from the page to put it in freepage array
				freePagesPathes.add(path);			
			}
		}

		numOfRows--;

		serializeTable();
		
	}
	
	public void insertIntoPage(Hashtable<String,Object> htblColNameValue) throws DBAppException {
		
		String pagePath;
		
		if(this.freePagesPathes.size() == 0)     //here we check if there is another page free other than the last one
			pagePath = getLastPagePath();
		else {
			pagePath = freePagesPathes.get(0);   //if we found a page we use it 
		}
		
		if(primaryKeyExists(htblColNameValue.get(primaryKeyName)) != null)
			throw new DBAppException("This key already exists!");	
		else
			primaryKeys.add(htblColNameValue.get(primaryKeyName));
	    
		Page page = Page.deserializePage(pagePath);
        page.insertRow(htblColNameValue);

        if(page.getNumOfRows() == maxPageRowNumber) {
        	if(pagePath.equals(lastPagePath))
        		createPage();
        	else
        		freePagesPathes.remove(pagePath);
        }
		
        page.serializePage(pagePath);
        
        
      
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
			e.printStackTrace();
		} catch (IOException e) {
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
            e.printStackTrace();
        }
		
		return table;
	}

	
	private Object primaryKeyExists(Object newKey) {
		Object reqKey = null;
		for (Object key : primaryKeys) {
			if(newKey.equals(key)) {
				reqKey = key;
				break;
			}
		}
		return reqKey;
	}
	
	public boolean isIndexed(String strColumnName) {
		//TODO 11 depends on indexing way later
		try {
			FileReader metaReader = new FileReader(new File(META_DATA_DIR));
			BufferedReader metaBuffer = new BufferedReader(metaReader);
			String line = null;
			while((line = metaBuffer.readLine()) != null) {
				String[] lineSplit = line.split(",");
				String currentTable = lineSplit[0].replaceAll(" ", "");
				String currentColumn = lineSplit[1].replaceAll(" ", "");
				if(currentTable.equals(this.tableName)
						&& currentColumn.equals(strColumnName)) {
					String indexed = lineSplit[4].replaceAll(" ", "");
					return (indexed.equals("true"));
				}
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public void updateFromPage(String strKey, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		
		Page page = null;

		String primaryKeyType = ColName_Type.get(this.primaryKeyName);
		
		Object key = htblColNameValue.get(primaryKeyName);
		
		if(key != null) {
			if(primaryKeyExists(htblColNameValue.get(primaryKeyName)) != null)
				throw new DBAppException("cannot change the primary key value to this value because it already exists!");
			else
			{
				primaryKeys.remove(strKey);
				primaryKeys.add(htblColNameValue.get(primaryKeyName));
				
			}
		}

		
		for (String path : pagePathes) {
			page = Page.deserializePage(path);
			page.updateRow(strKey , this.primaryKeyName ,htblColNameValue, primaryKeyType);
			page.serializePage(path);
		}
		
		serializeTable();
		
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

	public String getColumnType(String colName) {
	    return ColName_Type.get(colName);
	}
	
	public void setColumnIndexed(String colName) {
	    this.ColName_Indexed.remove(colName);
	    this.ColName_Indexed.put(colName, true);
	}
	
	public boolean isColumnIndexed(String colName) {
	    return this.ColName_Indexed.get(colName);
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
	
}
