package teamName;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

public class DBApp {
	
	Hashtable<String, ArrayList<Page>> nameAllTables;
	
	public DBApp() throws IOException {
		
		nameAllTables = new Hashtable<String, ArrayList<Page>>();
		
	}

	public void init() {
		
		// TODO this does whatever initialization you would like
		// no idea what should we do with this, as we have already a constructor !
	
	}

	public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType)
			throws DBAppException {
		
		//TODO
		
		Page table = new Page (strTableName, strClusteringKeyColumn, htblColNameType);
		
		if(!nameAllTables.contains("strTableName")) {
			
			nameAllTables.put(strTableName, new ArrayList<Page>());
			
		}
		
		nameAllTables.get("StrTableName").add(table);

	
	}

	public void createBRINIndex(String strTableName, String strColName)
			throws DBAppException {
		
		//TODO
	
	}

	public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) 
			throws DBAppException {
		
		Page target = null;
		
		target = nameAllTables.get(strTableName).get(-1);
		
		if(target != null) {
			if(target.getNumOfRows() < 250 ) {
				target.insertRow(htblColNameValue);
			}else {
				//TODO create another table with the same name somehow !
			}
		}

	
		
	}

	public void updateTable(String strTableName, String strKey, Hashtable<String,Object> htblColNameValue )
			throws DBAppException {
		
		//TODO
	
	}

	public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue)
			throws DBAppException {
		
		//TODO
	
	}

	public Iterator selectFromTable(String strTableName, String strColumnName, Object[] objarrValues,
			String[] strarrOperators) throws DBAppException {

		
		//TODO
		
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
	
}



