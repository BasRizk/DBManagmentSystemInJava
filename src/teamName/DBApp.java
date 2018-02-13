package teamName;

import java.util.Hashtable;

public class DBApp {
    
    public void init( )     // this does whatever initialization you would like
    {
        
    }
    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType ) throws DBAppException
    {
        
    }
    public void createBRINIndex(String strTableName, String strColName) throws DBAppException
    {
        
    }
    public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException
    {
        
    }
    public void updateTable(String strTableName, String strKey, Hashtable<String,Object> htblColNameValue ) throws DBAppException
    {
        
    }
    public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException
    {
        
    }
    public Iterator selectFromTable(String strTableName, String strColumnName, Object[] objarrValues, String[] strarrOperators) throws DBAppException
    {
        
    }

}
