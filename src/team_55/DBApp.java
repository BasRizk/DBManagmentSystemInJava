package team_55;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;

/**
 * A Basic Database Engine that supports Brin Indexing,
 * Developed during the course work of DATABASES II 
 * CSEN 603 in the German University in Cairo
 * 
 * Main Class : DBApp
 * 
 * @authors Basem Rizk, Michael Khalil, Steven Nassef, and Ibram Abdel Malek
 *
 * 
 *
 */

public class DBApp {
	
	public ArrayList<Table> tables;
	private static int maximumRowsCountinPage = 100;
	private static int mBRINSize = 15;
	private final static String META_DATA_DIR = "data\\metadata.csv";
	private final static String TABLES_DIR = "Tables/";
	private final static String OUTER_SPARSE_DIR = "IndexPages/OuterSparsePages/";
	private final static String INNER_SPARSE_DIR = "IndexPages/InnerSparsePages/";
	private final static String DENSE_DIR = "IndexPages/DensePages/";
	
	/*
	 * Indexes are saved such in the following format:
	 * 
	 * TABLES_DIR + strTableName + "/" + strColumnName + "/" + OUTER_SPARSE_DIR
	 * 
	 * 
	 */

	public DBApp() {

		//TODO Make sure that every needed Directory is Created
		DBAppConfig config;
		try {
			config = new DBAppConfig();
			maximumRowsCountinPage = config.getmMaximumRowsCountinPage();
			mBRINSize = config.getmBRINSize();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		tables = new ArrayList<Table>();

		// Creating the global meta-data file

		String metadataPath = META_DATA_DIR;
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
			e1.printStackTrace();
		}

		File folder = new File(TABLES_DIR);
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
	
	private void updateBRINIndex(String strTableName) throws DBAppException {
		
		Table table = tableExists(strTableName);
		
		table.getColName_Type().keySet();
		
		for (String key : table.getColName_Type().keySet()) {
			if(table.isIndexed(key)) {
				File tableDir;
				String outerSparsePagesDir = TABLES_DIR + strTableName + "/" + key + "/" + OUTER_SPARSE_DIR;
				tableDir = new File(outerSparsePagesDir);
				tableDir.delete();
				String innerSparsePagesDir = TABLES_DIR + strTableName + "/" + key + "/" + INNER_SPARSE_DIR;
				tableDir = new File(innerSparsePagesDir);
				tableDir.delete();
				String densePagesDir = TABLES_DIR + strTableName + "/" + key + "/" + DENSE_DIR;
				tableDir = new File(densePagesDir);
				tableDir.delete();
				createBRINIndex(strTableName, key);
			}
		}
	}
	
	public void createBRINIndex(String strTableName, String strColName)
			throws DBAppException {
		
		init();
		Table targetTable = tableExists(strTableName);
		
		if (targetTable == null)
			throw new DBAppException("table does not exist!");
		else {
			
			
			File tableDir;
			String outerSparsePagesDir = TABLES_DIR + strTableName + "/" + strColName + "/" + OUTER_SPARSE_DIR;
			tableDir = new File(outerSparsePagesDir);
			tableDir.mkdirs();
			String innerSparsePagesDir = TABLES_DIR + strTableName + "/" + strColName + "/" + INNER_SPARSE_DIR;
			tableDir = new File(innerSparsePagesDir);
			tableDir.mkdirs();
			String densePagesDir = TABLES_DIR + strTableName + "/" + strColName + "/" + DENSE_DIR;
			tableDir = new File(densePagesDir);
			tableDir.mkdirs();

			
			Page page = null;

			targetTable.setColumnIndexed(strColName);
	        String colType = targetTable.getColumnType(strColName);
			
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
			ArrayList<BrinSparsePage> sparsePagesFirstLevel = createSparseLevel(densePages, densePagesDir);
			ArrayList<BrinSparsePage> sparsePagesSecondLevel = createSecondSparseLevel(sparsePagesFirstLevel, innerSparsePagesDir);

			serializeAllSparsePages(sparsePagesSecondLevel, outerSparsePagesDir);
			serializeAllSparsePages(sparsePagesFirstLevel, innerSparsePagesDir);
			serializeAllDensePages(densePages, densePagesDir);
			
		}
	}
	
	private void serializeAllSparsePages(ArrayList<BrinSparsePage> sparsePages, String sparsePagesDir) {
		for(int i=0; i<= sparsePages.size(); i++) {
			sparsePages.get(i).serializeBrinSparsePage(sparsePagesDir + "sparsePage_" + i + ".ser");
		}
	}
	
	private void serializeAllDensePages(ArrayList<DensePage> densePages, String densePagesDir) {
		for(int i=0; i<= densePages.size(); i++) {
			densePages.get(i).serializeDensePage(densePagesDir + "densePage_" + i + ".ser");
		}
	}

	private static ArrayList<BrinSparsePage> createSecondSparseLevel(ArrayList<BrinSparsePage> sparsePages, String beforePagesDir){
		ArrayList<BrinSparsePage> secondLevelSparsePages = new ArrayList<BrinSparsePage>();
		int counterOfSparsePages = 0;
		for (BrinSparsePage sparsePage : sparsePages) {
			if(secondLevelSparsePages.get(secondLevelSparsePages.size() - 1).getSize() == mBRINSize)
				secondLevelSparsePages.add(new BrinSparsePage());
			BrinSparsePage lastPage = secondLevelSparsePages.get(secondLevelSparsePages.size() - 1);
			lastPage.getMinIndexCol().add(sparsePage.getMin(0));
			lastPage.getMaxIndexCol().add(sparsePage.getMax(sparsePage.getSize() - 1));
			lastPage.getRefCol().add(beforePagesDir + "sparsePage_" + counterOfSparsePages + ".ser");
			counterOfSparsePages++;
		}
		return secondLevelSparsePages;
	}
	
	private static ArrayList<BrinSparsePage> createSparseLevel(ArrayList<DensePage> densePages, String beforePagesDir){
		ArrayList<BrinSparsePage> sparsePages = new ArrayList<BrinSparsePage>();
		int counterOfDensePages = 0;
		for (DensePage densePage : densePages) {
			if(sparsePages.get(sparsePages.size() - 1).getSize() == mBRINSize)
				sparsePages.add(new BrinSparsePage());
			BrinSparsePage lastPage = sparsePages.get(sparsePages.size() - 1);
			lastPage.getMinIndexCol().add(densePage.getIndex().get(0));
			lastPage.getMaxIndexCol().add(densePage.getIndex().get(densePage.getIndex().size() - 1));
			lastPage.getRefCol().add(beforePagesDir + "densePage_" + counterOfDensePages + ".ser");
			counterOfDensePages++;
		}
		return sparsePages;
	}

	private static void insertIntoDensePage(ArrayList<DensePage> densePages,Object value,Tuple tuple, String colType){
		//boolean inserted = false;
		if(densePages.size() == 0){
			DensePage firstPage = new DensePage(colType);
			densePages.add(firstPage);
			firstPage.insertInDenseIndex(value, tuple);
			return;
		}
		int i = 0;
		for (DensePage densePage : densePages) {
			ArrayList<Object> index = densePage.getIndex();
			if((compareWithAllTypes(value, densePage.getIndex().get(0),">=", colType) && compareWithAllTypes(value, densePage.getIndex().get(densePage.getIndex().size() - 1),"<=", colType)) || compareWithAllTypes(value, densePage.getIndex().get(0),"<", colType)) {
				if(densePage.getSize() < mBRINSize) {
					densePage.insertInDenseIndex(value, tuple);
					//inserted = true;
				}
				else{
					Object val = index.get(index.size() - 1);
					Tuple tuple2 = densePage.getTuples(val).get(densePage.getTuples(val).size() - 1);
					densePage.deleteFromDenseIndex(val, tuple2);
					densePage.insertInDenseIndex(value, tuple);
					if(i == densePages.size() - 1){
						densePages.add(new DensePage(colType));
						densePages.get(densePages.size() - 1).insertInDenseIndex(val, tuple2);
					}
					//inserted = true;
					else
						insertIntoDensePage((ArrayList<DensePage>)densePages.subList(i + 1, densePages.size()),val,tuple2, colType);
				}
				break;
			}
			i++;
			
		}
		/*
		if(!inserted) {
		    densePages.add(new DensePage(colType));
            insertIntoDensePage(densePages, value, tuple, colType);
		}
		*/
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

		if (table != null) {
			table.insertIntoPage(htblColNameValue);
			updateBRINIndex(strTableName);
		}
		else
			throw new DBAppException("Table does not exist!");

	}

	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {

		this.init();
		Table table = tableExists(strTableName);

		if (table != null) {
			table.updateFromPage(strKey, htblColNameValue);
			updateBRINIndex(strTableName);
		}
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
			updateBRINIndex(strTableName);
		}
	}

	public Iterator<Tuple> selectFromTable(String strTableName, String strColumnName, Object[] objarrValues,
			String[] strarrOperators) throws DBAppException {

		init();
		
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
	
	private Iterator<Tuple> selectUsingExtensiveSearching(String strTableName, String strColumnName, Object[] objarrValues,
			String[] strarrOperators) {
		
		//TODO Do something to check if columnName actually exists in the table
		
		ArrayList<Tuple> selectedTuples = new ArrayList<Tuple>();
		String type = getTypeOf(strTableName, strColumnName);

		
		Table targetTable = tableExists(strTableName);
		if(targetTable != null) {
			
			ArrayList<String> pagePathes = targetTable.getPagePathes();
			for(String pagePath: pagePathes) {
				Page page = Page.deserializePage(pagePath);
				for(Tuple tuple : page.getRows()) {
					
					Object tupleValue = tuple.getValueOf(strColumnName);
					boolean[] isItHere = new boolean[objarrValues.length];
					isItHere[0] = true; isItHere[1]= true;
					
					for (int i = 0; i < 2; i++) {

						
						Object compareValue = objarrValues[i];
						
						isItHere[i] = compareWithAllTypes(tupleValue,
										compareValue,
										strarrOperators[i], type);
						
					}
					
					if(isItHere[0] && isItHere [1]) {
						selectedTuples.add(tuple);
					}
				}
				
			}
			
		}
		
		if(!selectedTuples.isEmpty()) {
			return (Iterator<Tuple>) selectedTuples;
		}

		return null;
	}

	private Iterator<Tuple> selectUsingBrinIndex(String strTableName, String strColumnName, Object[] objarrValues,
			String[] strarrOperators, ArrayList<Object> selectedBackPages, boolean sparseLevel) {
		
		ArrayList<Object> selectedDensePages = new ArrayList<>();
		ArrayList<Object> selectedSparsePages = new ArrayList<>();
		
		String type = getTypeOf(strTableName, strColumnName);
		String[] listOfPathes;
		
		if(selectedBackPages == null) {
			File folder = new File(TABLES_DIR + strTableName + "/" + strColumnName + "/" + OUTER_SPARSE_DIR);
			File[] listOfFiles = folder.listFiles();
			listOfPathes = getPathes(listOfFiles);
		} else {
			listOfPathes = (String[]) selectedBackPages.toArray();
		}

		Arrays.sort(listOfPathes);
		
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

							Object compareValue = objarrValues[i];
							
							isItHere[i] = compareWithAllTypes((Object)sparsePage.getMin(sp),
											(Object) sparsePage.getMax(sp),
											(Object)compareValue,
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
				
				if(!selectedTuples.isEmpty()) {
					return (Iterator<Tuple>) selectedTuples;
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

	private boolean compareWithAllTypes(Object min, Object max, Object currentValue, String operator, String type) {
		
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
	
	private static boolean compareWithAllTypes(Object denseValue, Object compareValue, String operator, String type) {
		
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

	private static boolean compareWith(Double value, Double compareValue, String operator) {
	
		switch(operator) {
		
		case ">": return (value > compareValue);
		case ">=": return (value >= compareValue);
		case "<": return (value < compareValue);
		case "<=": return (value <= compareValue);
		
		}
		return false;
	}
	
	private static boolean compareWith(String value, String compareValue, String operator) {
		
		switch(operator) {
		
		case ">": return (value.compareTo(compareValue) > 0);
		case ">=": return (value.compareTo(compareValue) >= 0);
		case "<": return (value.compareTo(compareValue) < 0);
		case "<=": return (value.compareTo(compareValue) <= 0);
		
		}
		return false;
	}
	
	private static boolean compareWith(boolean denseValue, boolean compareValue, String operator) {
		//No support for boolean Indexing
				return false;
	}
	
	private static boolean compareWith(Date value, Date compareValue, String operator) {
		
		switch(operator) {
		
		case ">": return (value.compareTo(compareValue) > 0);
		case ">=": return (value.compareTo(compareValue) >= 0);
		case "<": return (value.compareTo(compareValue) < 0);
		case "<=": return (value.compareTo(compareValue) <= 0);
		
		}
		return false;
	}

	private String getTypeOf(String strTableName, String strColumnName) {

		Table targetTable = tableExists(strTableName);
		if(targetTable != null)
			return targetTable.getColumnType(strColumnName);
		else
			return null;
	}

	private boolean brinIndexed(String strTableName, String strColumnName) {
		
		try {
			FileReader metaReader = new FileReader(new File(META_DATA_DIR));
			BufferedReader metaBuffer = new BufferedReader(metaReader);
			String line = null;
			while((line = metaBuffer.readLine()) != null) {
				String[] lineSplit = line.split(",");
				String currentTable = lineSplit[0].replaceAll(" ", "");
				String currentColumn = lineSplit[1].replaceAll(" ", "");
				if(currentTable.equals(strTableName)
						&& currentColumn.equals(strColumnName)) {
					String indexed = lineSplit[4].replaceAll(" ", "");
					metaBuffer.close();
					return (indexed.equals("true"));
				}
			}
			metaBuffer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	private Table tableExists(String tableName) {

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
