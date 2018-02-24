package teamName;

import java.util.Hashtable;

public class DBAppTest {
	
	public static void main(String[]args) throws DBAppException {
		DBApp app = new DBApp();
		//app.init();
		
		String strTableName = "Student";
		
		Hashtable<String, String> htblColNameType = new Hashtable<String, String>( );
		
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		
		app.createTable( strTableName, "id", htblColNameType );
		
		
		//Insertion into Student Table
		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>( );
		
		htblColNameValue.put("id", new Integer( 2343432 ));
		htblColNameValue.put("name", new String("Ahmed Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.95 ) );
		
		app.insertIntoTable( strTableName , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 453455 ));
		htblColNameValue.put("name", new String("Ahmed Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.95 ) );
		
		app.insertIntoTable( strTableName , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 5674567 ));
		htblColNameValue.put("name", new String("Dalia Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.25 ) );
		
		app.insertIntoTable( strTableName , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 23498 ));
		htblColNameValue.put("name", new String("John Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		
		app.insertIntoTable( strTableName , htblColNameValue );
		
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 78452 ));
		htblColNameValue.put("name", new String("Zaky Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.88 ) );
		
		app.insertIntoTable( strTableName , htblColNameValue );
		app.deleteFromTable( strTableName , htblColNameValue);
		
		//Another table
		/*
		String strTableName2 = "Student2";
		Hashtable<String, String> htblColNameType2 = new Hashtable<String, String>( );
		htblColNameType2.put("id", "java.lang.Integer");
		htblColNameType2.put("name", "java.lang.String");
		htblColNameType2.put("gpa", "java.lang.Double");
		
		app.createTable( strTableName2, "id", htblColNameType2 );*/
		app.printDB();
		
		
	}
}
