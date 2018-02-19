package teamName;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

public class Page implements Serializable {

	/**
	 *	Having a fixed serialVersionUID to be able to Deserialize objects on any version of java
	 */
	private static final long serialVersionUID = 8010842808275391794L;
	
	private String tableName;
	private String primaryKey;
	private Hashtable<String, String> ColName_Type;
	private int numOfCol;
	
	private ArrayList<Tuple> rows;
	private int numOfRows;
	
	public Page(String strtableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType, boolean firstTable) {
		
		this.tableName = strtableName;
		this.primaryKey = strClusteringKeyColumn;
		
		this.ColName_Type = htblColNameType;
		this.ColName_Type.put("TouchData","java.util.Date");
		
		//TODO 9 - Check that each column has a valid type
		// Supported Types: java.lang.Integer, java.lang.String, java.lang.Double, 
		// java.lang.Boolean and java.util.Date
		
		
		this.numOfCol = ColName_Type.size();

		this.rows = new ArrayList<Tuple>();
		this.numOfRows = rows.size();
		
		
		//TODO 10 Meta data if first instance of table
		
		if(firstTable) {
			this.createMetadataFile();
		}
		
		
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
			
			data = data +  tableName + ", " + colName + ", " + colType + ", " +
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
	
	public void insertRow(Hashtable<String, Object> htblColNameValue) {
		// TODO 10 insertRow 
		
	}
	
	public boolean isIndexed(String colName) {
		//TODO 11 depends on indexing way later
		return false;
	}

	public String getPageName() {
		return tableName;
	}


	public void setPageName(String tableName) {
		this.tableName = tableName;
	}


	public String getPrimaryKey() {
		return primaryKey;
	}

	
	public int getNumOfCol() {
		return numOfCol;
	}
	
	public int getNumOfRows() {
		return numOfRows;
	}



}