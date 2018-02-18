package teamName;

public class DBAppException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5994129995067077521L;
	
	public DBAppException (){
		super("there is unhandled DBApp Exception");
	}
	public DBAppException(String Message){
		super(Message);
	}
}
