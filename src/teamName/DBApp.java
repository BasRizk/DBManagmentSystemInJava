package teamName;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

public class DBApp {
	

	public void init() {
		
		// TODO this does whatever initialization you would like
		// no idea what should we do with this, as we have already a constructor !
	
	}

	public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType)
			throws DBAppException {
		
		
		
		//TODO 1 Check if Page's table already existed before
		
		int numOfPages = getNumOfPages(strTableName);
		boolean firstTable = (numOfPages == 0)? true: false;
		

		Page page = new Page (strTableName, strClusteringKeyColumn, htblColNameType, firstTable);
		
		if(getNumOfPages(strTableName) == 0) {
			
			try {
				FileOutputStream fos = new FileOutputStream(strTableName + ".ser");
				ObjectOutputStream oos;
				oos = new ObjectOutputStream(fos);
				oos.writeObject(page);
				oos.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
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
		
		Page target = null;
		
		//TODO 3 Loop over all Pages (files) and get the target table
		
		if(target != null) {
			if(target.getNumOfRows() < 200 ) {
				target.insertRow(htblColNameValue);
				
			}else {
				//TODO 4 create another table with the same name somehow !
			}
		}

	
		
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
	
	private static int getNumOfPages(String strTableName) {
		
		//TODO 8 getNumOfPages following tableName , return 0 if non
		
		return 0;
	}
	
}



