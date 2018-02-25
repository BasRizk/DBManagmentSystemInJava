package teamName;

import java.util.Hashtable;

public class DBAppTest2 {
	
	public static void main(String[]args) throws DBAppException {
		DBApp app = new DBApp();
		
		String strTableName = "Emp";
        
        Hashtable<String, String> htblColNameType = new Hashtable<String, String>( );
        
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");
        
        app.createTable( strTableName, "id", htblColNameType );
        
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
        htblColNameValue.put("id", new Integer( 45340055 ));
        htblColNameValue.put("name", new String("Ahmed Noor" ) );
        htblColNameValue.put("gpa", new Double( 0.95 ) );
        
        app.insertIntoTable( strTableName , htblColNameValue );
        
        htblColNameValue.clear( );
        htblColNameValue.put("id", new Integer( 45131455 ));
        htblColNameValue.put("name", new String("Ahmed Noor" ) );
        htblColNameValue.put("gpa", new Double( 0.95 ) );
        
        app.insertIntoTable( strTableName , htblColNameValue );
        
        
        htblColNameValue.clear( );
        htblColNameValue.put("id", new Integer( 5674567 ));
        htblColNameValue.put("name", new String("Dalia Noor" ) );
        htblColNameValue.put("gpa", new Double( 1.25 ) );
        
        app.deleteFromTable( strTableName , htblColNameValue );
       
        
        htblColNameValue.clear( );
        htblColNameValue.put("name", new String("Ahmed Noor2" ) );
        htblColNameValue.put("gpa", new Double( 0.96 ) );
        
        app.updateTable( strTableName , "453455", htblColNameValue );
        
        
		
		app.printDB();
		
		//Page page = Page.deserializePage("TablePages/Student/Student_1.page");
		
		//page.printPage();
		
	}

	
	
}
