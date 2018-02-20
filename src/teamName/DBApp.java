package teamName;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;

public class DBApp {
	
	private ArrayList<Table> tables;

	public void init() {
		
		// TODO this does whatever initialization you would like
		// no idea what should we do with this, as we have already a constructor !
		

			
    	try {
    		File folder = new File("../Tables/");
    		File[] listOfFiles = folder.listFiles();
    		FileInputStream fileInput = null;
            ObjectInputStream in = null;
    	    for(File file : listOfFiles) {
    	    	String filename = file.getName();
				fileInput = new FileInputStream(filename);
				
		        in = new ObjectInputStream(fileInput);
				
				Table table = (Table) in.readObject();
		        tables.add(table);
    	    }    
	        in.close();
	        fileInput.close();
    	}
    	 catch(IOException ex) {
    		//TODO
            System.out.println("IOException is caught");
    	} catch(ClassNotFoundException ex) {
        	//TODO
            System.out.println("ClassNotFoundException is caught");
        }
	        
    } 
	
	

	public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType)
			throws DBAppException {
		
		//TODO 1 Check if Page's table already existed before

		//int numOfPages = getNumOfPages(strTableName).size();
		//boolean firstTable = (numOfPages == 0)? true: false;
		

		
		
		//Page page = new Page (strTableName, strClusteringKeyColumn, htblColNameType, firstTable);
		
		if(!tableExists(strTableName)) {       //getNumOfPages(strTableName).size() != 0) {
			
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
		
	    Table table = null;
	    Date date = new Date();
	    htblColNameValue.put("TouchDate", date);
	    
		//TODO 3 Loop over all Pages (files) and get the target table
		
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
		
		
        /*String pagePath = table.getLastPagePath();
        Page page = 
        
		if( != null) {
			if(target.getNumOfRows() < 200 ) {
				page.insertRow(htblColNameValue);
			}else {
				//4 create another table with the same name somehow !
			}
		}*/

	
		
	}

	public void updateTable(String strTableName, String strKey, Hashtable<String,Object> htblColNameValue )
			throws DBAppException {
		
		//TODO 5 updateTable
	
	}

	public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue)
			throws DBAppException {
		
		//TODO 6 deleteFromTable
	
	}

	public Iterator selectFromTable(String strTableName, String strColumnName, Object[] objarrValues,
			String[] strarrOperators) throws DBAppException {

		
		//TODO 7 selectFromTable
		
		/*
		 
		// this part was used to deserialize all files,
		// however, we should do that on demand
		// (Commented for reference)
		
		File folder = new File("../bin/teamName/");
		File[] listOfFiles = folder.listFiles();
		FileInputStream fileInput = null;
        ObjectInputStream in = null;
	    for(File file : listOfFiles) {
	    	String filename = file.getName();
			
	    	try {
				fileInput = new FileInputStream(filename);
				
		        in = new ObjectInputStream(fileInput);
				
				Table table = (Table) in.readObject();
		        tables.add(table);
		        
	    	} catch(IOException ex) {
	    		//TODO
	            System.out.println("IOException is caught");
	    	} catch(ClassNotFoundException ex) {
	        	//TODO
	            System.out.println("ClassNotFoundException is caught");
	        }
	        
	    }
	    
        in.close();
        fileInput.close();
		 */

		
		return null;

	}
	
	private boolean tableExists(String tableName) {
		for (Table table : tables) {
			if(table.getName().equals(tableName))
				return true;
		}
		return false;
	}
	
	private static ArrayList<Page> getNumOfPages(String strTableName) {
		
		ArrayList<Page> tablePages = new ArrayList<Page>();
		
		
		//TODO 8 getNumOfPages following tableName , return 0 if non
		
		return tablePages;
	}
	
}



