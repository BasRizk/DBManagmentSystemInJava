package teamName;

import java.util.Hashtable;

public class Testing {
	
	public static void main(String[]args) {
		DBApp app = new DBApp();
		//app.init();
		
		String strTableName = "Student";
		Hashtable htblColNameType = new Hashtable( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		
		try {
			app.createTable( strTableName, "id", htblColNameType );
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String strTableName2 = "Student2";
		Hashtable htblColNameType2 = new Hashtable( );
		htblColNameType2.put("id", "java.lang.Integer");
		htblColNameType2.put("name", "java.lang.String");
		htblColNameType2.put("gpa", "java.lang.Double");
		
		try {
			app.createTable( strTableName2, "id", htblColNameType2 );
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
