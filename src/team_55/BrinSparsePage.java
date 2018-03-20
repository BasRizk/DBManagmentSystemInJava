package team_55;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * A Page simulating a sparseIndex pointing toward either:
 * a (DensePage) or a (BrinSparsePage)
 * aiming to develop a BrinIndex 
 *
 * 
 * @author Basem Rizk
 *
 */

public class BrinSparsePage implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ArrayList<Object> minIndexCol;
	private ArrayList<Object> maxIndexCol;
	private ArrayList<?> refCol;
	private int level;
	
	public BrinSparsePage(String typeOfRef,int level) {
		this.minIndexCol = new ArrayList<Object>();
		this.maxIndexCol = new ArrayList<Object>();
		
		switch(typeOfRef) {
		case "DensePage":
			this.refCol = new ArrayList<DensePage>();
			break;
		case "BrinSparsePage":
			this.refCol = new ArrayList<BrinSparsePage>();
			break;
		default:
			this.refCol = new ArrayList<>();
		}
		this.level = level;
	}
	
	public void serializeBrinSparsePage(String sparsePagePath) {
		
		try {
			
            FileOutputStream fos = new FileOutputStream(sparsePagePath);
            ObjectOutputStream oos;
            oos = new ObjectOutputStream(fos);
            oos.writeObject(this);
            oos.close();
            fos.close();
	        
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static BrinSparsePage deserializeBrinSparsePage(String sparsePagePath) {
		BrinSparsePage page = null;
		
		try {
	        
            FileInputStream fis = new FileInputStream(sparsePagePath);
            ObjectInputStream ois;
            ois = new ObjectInputStream(fis);
            page = (BrinSparsePage) ois.readObject();
            ois.close();
            fis.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
		
		return page;
	}
	
	public Object getMin(int index) {
		return minIndexCol.get(index); 
	}
	
	public Object getMax(int index) {
		return maxIndexCol.get(index); 
	}
	
	public Object getRef(int index) {
		return refCol.get(index);
	}
	
	public int getIndexLevel() {
		return level;
	}
	
	public int getSize() {
		return minIndexCol.size();
	}
	
}
