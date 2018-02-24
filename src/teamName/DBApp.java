package teamName;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;

public class DBApp {
	
	private ArrayList<Table> tables;
	
	public DBApp() {
		tables = new ArrayList<Table>();
		
		
		//Creating the global meta-data file

		String metadataPath = "data\\metadata.csv";
		File metadataFile = new File(metadataPath);
		
		
		if(!metadataFile.exists()) {
			try {
				metadataFile.getParentFile().mkdirs(); 
				metadataFile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		

	}
	
	public void init() {

		File folder = new File("Tables/");
		File[] listOfFiles = folder.listFiles();
		
        if(listOfFiles != null) {
    	    for(File file : listOfFiles) {
    	    	String filePath = file.getPath();

    	    	Table table = Table.deserializeTable(filePath);
    	        tables.add(table);
    	    }    
        }

    } 
	
	

	public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType)
			throws DBAppException {
		
		if(!tableExists(strTableName)) {      
			
			Table table = new Table(strTableName, strClusteringKeyColumn, htblColNameType , 200);
			tables.add(table);
			
		} else {
			
			throw new DBAppException("This table already exists.");
			
		}		
	
	}

	public void createBRINIndex(String strTableName, String strColName)
			throws DBAppException {
		
		//TODO 2 createBRINindex
	
	}

	public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) 
			throws DBAppException {
		
		this.init();
		
	    Table table = null;
	    Date date = new Date();
	    htblColNameValue.put("TouchDate", date);
	    
		
		for (Table tableSearch : tables) {
            if(tableSearch.getName().equals(strTableName)) {
                table = tableSearch;
                break;
            }
		} 
		
		if (table!= null)
		    table.insertIntoPage(htblColNameValue);
		else
		    throw new DBAppException("Table does not exist!");
		
	}

	public void updateTable(String strTableName, String strKey, Hashtable<String,Object> htblColNameValue )
			throws DBAppException {
		
		//TODO 5 updateTable
	
	}

	public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue)
			throws DBAppException {
		
		//TODO 6 deleteFromTable
		this.init();
		Table table = null;
		for (Table tableToDelete : tables) {
			if(tableToDelete.getName().equals(strTableName)){
				table = tableToDelete;
				break;
				}			
			}
		if(table == null){
			throw new DBAppException("Table does not exist");
		}
		else{
			table.deleteFromPage(htblColNameValue);
		}
	}

	@SuppressWarnings("rawtypes")
	public Iterator selectFromTable(String strTableName, String strColumnName, Object[] objarrValues,
			String[] strarrOperators) throws DBAppException {

		
		//TODO 7 selectFromTable
		
		return null;

	}
	
	private boolean tableExists(String tableName) {
		this.init();
		for (Table table : tables) {
			if(table.getName().equals(tableName))
				return true;
		}
		return false;
	}
	
	public void printDB() {
		this.init();
		System.out.println("This is all you have in the database: " + "\n");
		for( Table table : tables) {
			System.out.println("\n" + " -----TABLE " + table.getName() + " STARTED------" + "\n");
			table.printTableData();
			System.out.println("\n" + " -----TABLE " + table.getName() + " FINSHED------" + "\n");
		}
		System.out.println("Everything has been printed.");
	}
}



