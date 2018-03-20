package team_55;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;

public class DBApp {
	
	public ArrayList<Table> tables;
	private static int maximumRowsCountinPage = 100;
	private int mBRINSize = 15;

	public DBApp() {

		DBAppConfig config;
		try {
			config = new DBAppConfig();
			maximumRowsCountinPage = config.getmMaximumRowsCountinPage();
			mBRINSize = config.getmBRINSize();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		tables = new ArrayList<Table>();

		// Creating the global meta-data file

		String metadataPath = "data\\metadata.csv";
		File metadataFile = new File(metadataPath);

		if (!metadataFile.exists()) {
			try {
				metadataFile.getParentFile().mkdirs();
				metadataFile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public void init() {

		DBAppConfig config;
		try {
			config = new DBAppConfig();
			maximumRowsCountinPage = config.getmMaximumRowsCountinPage();
			mBRINSize = config.getmBRINSize();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		File folder = new File("Tables/");
		File[] listOfFiles = folder.listFiles();
		tables.clear(); // clearing the array to avoid having duplicates

		if (listOfFiles != null) {
			for (File file : listOfFiles) {
				String filePath = file.getPath();

				Table table = Table.deserializeTable(filePath);
				tables.add(table);
			}
		}

	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {

		this.init();

		if (tableExists(strTableName) == null) {

			Table table = new Table(strTableName, strClusteringKeyColumn, htblColNameType, maximumRowsCountinPage);
			tables.add(table);

		} else {

			throw new DBAppException("This table already exists.");

		}

	}

	public void createBRINIndex(String strTableName, String strColName)
			throws DBAppException {
		
        String colType = (tableExists(strTableName).getColumnType(strColName));
		//TODO 2 createBRINindex
		Table targetTable = tableExists(strTableName);
		if (targetTable == null)
			throw new DBAppException("table does not exist!");
		else {
			// Creating index goes here
			Page page = null;

			ArrayList<DensePage> densePages = new ArrayList<DensePage>();
			for (String path : targetTable.getPagePathes()) {
				page = Page.deserializePage(path);
				for (Tuple tuple : page.getRows()) {
					//TODO adding exception if user entered wrong column name
					Object value = tuple.getColNameValue().get(strColName);
					insertIntoDensePage(densePages,value,tuple, colType);
				}
				page.serializePage(path);
			}
		}
	}
	
	private static void insertIntoDensePage(ArrayList<DensePage> densePages,Object value,Tuple tuple, String colType){
		String stringValue = (String) value;
		boolean inserted = false;
		
		for (DensePage densePage : densePages) {
			ArrayList<Object> index = densePage.getIndex();
			if(stringValue.compareTo((String)index.get(0)) > 0 && stringValue.compareTo((String)index.get(index.size())) <= 0){
				if(densePage.getSize() < maximumRowsCountinPage) {
					densePage.insertInDenseIndex(value, tuple);
					inserted = true;
				}
				else{
					Object val = index.get(index.size());
					densePage.getIndex().remove(index.size());
					Tuple tuple2 = densePage.getTuples(val).get(densePage.getTuples(val).size());
					densePage.deleteFromDenseIndex(val, tuple2);
					densePage.insertInDenseIndex(value, tuple);
					inserted = true;
					insertIntoDensePage(densePages,val,tuple2, colType);
				}
				break;
			}
		}
		
		if(!inserted) {
		    densePages.add(new DensePage(colType));
            insertIntoDensePage(densePages, value, tuple, colType);
		}
		
	}
	
	public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) 
			throws DBAppException {
		
		this.init();

		Table table = tableExists(strTableName);
		Date date = new Date();
		htblColNameValue.put("TouchDate", date);

		/*
		 * for (Table tableSearch : tables) { // implemented in tableExits Method
		 * if(tableSearch.getName().equals(strTableName)) { table = tableSearch; break;
		 * } }
		 */

		if (table != null)
			table.insertIntoPage(htblColNameValue);
		else
			throw new DBAppException("Table does not exist!");

	}

	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {

		this.init();
		Table table = tableExists(strTableName);

		if (table != null)
			table.updateFromPage(strKey, htblColNameValue);
		else
			throw new DBAppException("Table does not exist!");

	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

		this.init();

		Table table = tableExists(strTableName);

		if (table == null) {
			throw new DBAppException("Table does not exist");
		} else {
			table.deleteFromPage(htblColNameValue);
		}
	}

	@SuppressWarnings("rawtypes")
	public Iterator selectFromTable(String strTableName, String strColumnName, Object[] objarrValues,
			String[] strarrOperators) throws DBAppException {

		// TODO 7 selectFromTable
		if(tableExists(strTableName) != null) {

			if (brinIndexed(strTableName, strColumnName)) {
				return selectUsingBrinIndex(strTableName, strColumnName, objarrValues,
						strarrOperators, null, true);
			} else {
				
				return selectUsingExtensiveSearching(strTableName, strColumnName, objarrValues,
						strarrOperators);
			}
			
		}
		
		return null;

	}
	
	private Iterator selectUsingExtensiveSearching(String strTableName, String strColumnName, Object[] objarrValues,
			String[] strarrOperators) {
		// TODO Normal Selection from table
		
		ArrayList<Page> selectedPages = new ArrayList<>();

		return null;
	}

	private Iterator selectUsingBrinIndex(String strTableName, String strColumnName, Object[] objarrValues,
			String[] strarrOperators, ArrayList<Object> selectedBackPages, boolean sparseLevel) {
		
		ArrayList<Object> selectedDensePages = new ArrayList<>();
		ArrayList<Object> selectedSparsePages = new ArrayList<>();
		
		String type = getTypeOf(strTableName, strColumnName);
		String[] listOfPathes;
		
		if(selectedBackPages == null) {
			File folder = new File("/" + strTableName + "/" + strColumnName + "/OuterSparsePages/");
			File[] listOfFiles = folder.listFiles();
			listOfPathes = getPathes(listOfFiles);
		} else {
			listOfPathes = (String[]) selectedBackPages.toArray();
		}


		if (listOfPathes.length > 0) {
			
			if(sparseLevel) {
				for (int pathIndex = 0; pathIndex < listOfPathes.length; pathIndex++) {
					String filePath = listOfPathes[pathIndex];

					BrinSparsePage sparsePage = BrinSparsePage.deserializeBrinSparsePage(filePath);
					int pageSize = sparsePage.getSize();

					for (int sp = 0; sp < pageSize; sp++) {
						
						boolean[] isItHere = new boolean[objarrValues.length];
						isItHere[0] = true; isItHere[1]= true;
						
						for (int i = 0; i < 2; i++) {

							Object currentValue = objarrValues[i];
							
							isItHere[i] = compareWithAllTypes((Object)sparsePage.getMin(sp),
											(Object) sparsePage.getMax(sp),
											(Object)currentValue,
											strarrOperators[i], type);
							
						}
						
						if(isItHere[0] && isItHere[1]) { //You can loop over the array later if required
							
							Object ref = sparsePage.getRef(sp);
							if(ref instanceof DensePage) {
								selectedDensePages.add(sparsePage.getRef(sp));

							}else if(ref instanceof BrinSparsePage) {
								selectedSparsePages.add(sparsePage.getRef(sp));
							}
							
						} else {
							
							if(!selectedSparsePages.isEmpty()) {
								return selectUsingBrinIndex(strTableName, strColumnName, objarrValues,
										strarrOperators, selectedSparsePages, true);
							} else if(!selectedDensePages.isEmpty()) {
								return selectUsingBrinIndex(strTableName, strColumnName, objarrValues,
										strarrOperators, selectedSparsePages, false);
							}
						
						}
					}
				}
				
			} else { //Dense Level
				
				ArrayList<Tuple> selectedTuples = new ArrayList<Tuple>();

				
				for (int pathIndex = 0; pathIndex < listOfPathes.length; pathIndex++) {
					String filePath = listOfPathes[pathIndex];

					DensePage densePage = DensePage.deserializeDensePage(filePath);
					int pageSize = densePage.getSize();

					for (Object denseValue : densePage.getIndexColumn()) {

						boolean[] isItHere = new boolean[objarrValues.length];
						isItHere[0] = true; isItHere[1]= true;
						
						for (int i = 0; i < 2; i++) {
							
							Object compareValue = objarrValues[i];
							
							isItHere[i] = compareWithAllTypes(denseValue,
											(Object)compareValue,
											strarrOperators[i], type);

						}
						
						//TODO Look up the description for different order
						/*
						isItHere[0] = compareWithAllTypes(denseValue,
								(Object)objarrValues[0],
								strarrOperators[0], type);
						
						isItHere[1] = compareWithAllTypes(denseValue,
								(Object)objarrValues[0],
								strarrOperators[0], type);
						*/
						
						if(isItHere[0] && isItHere[1]) { //You can loop over the array later if required
							ArrayList<Tuple> ref = densePage.getTuples(denseValue);
							
							for(int i = 0; i<ref.size(); i++) {
								selectedTuples.add(ref.get(i));
							}
							
						}
					}
				}
				
			}
	
			
		}
		
		return null;

	}

	private String[] getPathes(File[] listOfFiles) {
		String[] pathes = new String [listOfFiles.length];
		for( int i = 0; i<listOfFiles.length; i++) {
			pathes[i] = listOfFiles[i].getPath();
		}
		return pathes;
	}

	private static boolean compareWithAllTypes(Object min, Object max, Object currentValue, String operator, String type) {
		
		switch (type) {
		
		case "java.lang.Integer":
		case "java.lang.Double":
			return compareWith((Double)min,
					(Double) max,
					(Double)currentValue,
					operator);

		case "java.lang.String":
			return compareWith((String) min,
					(String) max,
					(String) currentValue,
					operator);

		case "java.lang.Boolean":
			return compareWith((boolean) min,
					(boolean) max,
					(boolean) currentValue,
					operator);

		case "java.util.Date":
			return compareWith((Date) min,
					(Date) max,
					(Date) currentValue,
					operator);

		default:
			return false;
		}

	}

	//TODO if < and not found make it stop the other method.
	
	private static boolean compareWith(double min, double max, double value, String operator) {
		
		switch(operator) {
		
		case ">": return (value > min);
		case ">=": return (value >= min);
		case "<": return (value < max);
		case "<=": return (value <= max);
		
		}
		return false;
	}
	
	private static boolean compareWith(String min, String max, String value, String operator) {
		
		switch(operator) {
		
		case ">": return (value.compareTo(min) > 0);
		case ">=": return (value.compareTo(min) >= 0);
		case "<": return (value.compareTo(max) < 0);
		case "<=": return (value.compareTo(max) <= 0);
		
		}
		return false;
	}
	
	private static boolean compareWith(boolean min, boolean max, boolean value, String operator) {
		//No support for boolean Indexing
		return false;
	}
	
	private static boolean compareWith(Date min, Date max, Date value, String operator) {
		
		switch(operator) {
		
		case ">": return (value.compareTo(min) > 0);
		case ">=": return (value.compareTo(min) >= 0);
		case "<": return (value.compareTo(max) < 0);
		case "<=": return (value.compareTo(max) <= 0);
		
		}
		return false;
	}
	
	private boolean compareWithAllTypes(Object denseValue, Object compareValue, String operator, String type) {
		
		switch (type) {
		
		case "java.lang.Integer":
		case "java.lang.Double":
			return compareWith((Double) denseValue,
					(Double)compareValue,
					operator);

		case "java.lang.String":
			return compareWith((String) denseValue,
					(String) compareValue,
					operator);

		case "java.lang.Boolean":
			return compareWith((boolean) denseValue,
					(boolean) compareValue,
					operator);

		case "java.util.Date":
			return compareWith((Date) denseValue,
					(Date) compareValue,
					operator);

		default:
			return false;
		}
	}

	private boolean compareWith(Double value, Double compareValue, String operator) {
	
		switch(operator) {
		
		case ">": return (value > compareValue);
		case ">=": return (value >= compareValue);
		case "<": return (value < compareValue);
		case "<=": return (value <= compareValue);
		
		}
		return false;
	}
	
	private boolean compareWith(String value, String compareValue, String operator) {
		
		switch(operator) {
		
		case ">": return (value.compareTo(compareValue) > 0);
		case ">=": return (value.compareTo(compareValue) >= 0);
		case "<": return (value.compareTo(compareValue) < 0);
		case "<=": return (value.compareTo(compareValue) <= 0);
		
		}
		return false;
	}
	
	private boolean compareWith(boolean denseValue, boolean compareValue, String operator) {
		//No support for boolean Indexing
				return false;
	}
	
	private boolean compareWith(Date value, Date compareValue, String operator) {
		
		switch(operator) {
		
		case ">": return (value.compareTo(compareValue) > 0);
		case ">=": return (value.compareTo(compareValue) >= 0);
		case "<": return (value.compareTo(compareValue) < 0);
		case "<=": return (value.compareTo(compareValue) <= 0);
		
		}
		return false;
	}

	private String getTypeOf(String strTableName, String strColumnName) {
	 // TODO Get stColumnName type out of the meta-data csv file in the
        // strTableSection
		Table table = tableExists(strTableName);
		String type = table.getColumnType(strColumnName);
		return type;
	}

	private boolean brinIndexed(String strTableName, String strColumnName) {
		// TODO Check if strTableName has BRINindex on strColumnName from the meta-data
		// csv file
		return false;
	}

	private Table tableExists(String tableName) {
		// this.init(); //it makes stackoverflow , because in init method i call it so
		// it.
		Table reqTable = null;
		for (Table table : tables) {

			if (table.getName().equals(tableName))
				reqTable = table;
			else
				System.out.println(tableName + "............." + table.getName());
		}
		return reqTable;
	}

	public void printDB() {

		this.init();
		System.out.println("This is all you have in the database: " + "\n");
		for (Table table : tables) {
			System.out.println("\n" + " -----TABLE " + table.getName() + " STARTED------" + "\n");
			table.printTableData();
			System.out.println("\n" + " -----TABLE " + table.getName() + " FINSHED------" + "\n");
		}
		System.out.println("Everything has been printed.");
	}
}
