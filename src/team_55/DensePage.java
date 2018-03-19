package team_55;

import java.util.ArrayList;

/**
 * A class used to resemble a <tt>database dense index page</tt> through the use of
 * ArrayList of Objects for the index and another ArrayList of ArrayLists of tuples
 * to resemble the references to tuples.
 * 
 * Both ArrayLists have linear correspondence to each other.
 * 
 * @author Michael Khalil
 * @see ArrayList
 */

public class DensePage {
    
    private ArrayList<Object> index;                            // values of the column that the index is built on, should be sorted
    private ArrayList<ArrayList<Tuple>> tupleReferences;        /* sorted according to index Array such that each value in index array corresponds to
                                                                   an array of references to tuples matching that value */    
    
    public DensePage() {   
        tupleReferences = new ArrayList<>();
        index = new ArrayList<>();
    }

    
    /**
     * Returns an ArrayList of Tuples that are matching a value in the column that the DenseIndex is on
     * @param colValue is the column value of the tuples needed
     * @return Tuples with column value that matches colValue
     */
    public ArrayList<Tuple> getTuples(Object colValue) {
        return tupleReferences.get(index.indexOf(colValue));
    }
    
    
    /**
     * Insert tuple in the Dense Index
     * @param colValue is the the tuple's column value that the index is built on
     * @param tuple is the tuple that to be included in the Dense Index
     */
    public void insertInDenseIndex(Object colValue, Tuple tuple) {
        
        if(!index.contains(colValue)) {
            insertionSort(index, colValue);
            insertionSortForArray(tupleReferences, index.indexOf(colValue));
        }
        
        ArrayList<Tuple> tupleRefrencesPerIndex = tupleReferences.get(index.indexOf(colValue));
        
        tupleRefrencesPerIndex.add(tuple);

    }
    
    
    /**
     * Delete tuple from the Dense Index
     * @param colValue is the the tuple's column value that the index is built on
     * @param tuple is the tuple that to be deleted
     */
    public void deleteFromDenseIndex(Object colValue, Tuple tuple) {
        
        ArrayList<Tuple> tupleRefrencesPerIndex = tupleReferences.get(index.indexOf(colValue));
        
        tupleRefrencesPerIndex.remove(tuple);
        
        if(tupleRefrencesPerIndex.size() == 0)
            index.remove(colValue);

    }
    
// Update will better work from outside the class using delete followed by insert, because it might need to be inserted in different page    
//    /**
//     * Updates the value of the index of a Tuple
//     * @param colValueOld The old index column value
//     * @param colValueNew The new index column value
//     * @param tuple that is updated
//     */
//    public void updateDenseIndex(Object colValueOld, Object colValueNew, Tuple tuple) {
//        tupleReferences.get(index.indexOf(colValueOld)).remove(tuple);
//        
//        if(!index.contains(colValueNew)) {
//            insertionSort(index, colValueNew);
//            insertionSortForArray(tupleReferences, index.indexOf(colValueNew));
//        }
//        
//        tupleReferences.get(index.indexOf(colValueNew)).add(tuple);
//    }
//    
    
    
    /**
     * Inserts a value in an array list using insertion sort
     * @param array is the array list that the value will be inserted in
     * @param value is the value to be inserted in array
     */
    private static void insertionSort(ArrayList<Object> array, Object value) {          // TODO make it work for all Data Types
        
        int valueAfterParsing = Integer.parseInt((String) value);
        int positionOfInsertion = 0;
        
        for(Object indexInArray : array) {
            if( valueAfterParsing < (Integer.parseInt((String) indexInArray)) ) {
                positionOfInsertion = array.indexOf(indexInArray);
                break;
            }
        }
        
        for(int i = array.size(); i >= positionOfInsertion; i--) {
            array.set(i, array.get(i-1));
        }
        
        array.set(positionOfInsertion, valueAfterParsing);      // TODO Should we insert the value before parsing or after parsing?
        
    }
    
    
    /**
     * Inserts a new ArrayList in tupleRefrence array list according to the corresponding Index
     * @param array is the array list that the new array list will be inserted in
     * @param positionOfInsertion is the index that the new array list will be inserted in
     */
    private static void insertionSortForArray(ArrayList<ArrayList<Tuple>> array, int positionOfInsertion) {
        
        for(int i = array.size(); i >= positionOfInsertion; i--) {
            array.set(i, array.get(i-1));
        }
        
        array.set(positionOfInsertion, new ArrayList<>());
    }
    

}
