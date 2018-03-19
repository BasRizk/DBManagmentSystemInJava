package team_55;

import java.util.ArrayList;

public class DenseIndex {
    
    private ArrayList<Object> index;                            // values of the column that the index is built on, should be sorted
    private ArrayList<ArrayList<Tuple>> tupleReferences;        /* sorted according to index Array such that each value in index array corresponds to
                                                                   an array of references to tuples matching that value */    
    
    public DenseIndex() {   
        tupleReferences = new ArrayList<>();
        index = new ArrayList<>();
    }

    public ArrayList<ArrayList<Tuple>> gettupleReferences() {
        return tupleReferences;
    }

    public void settupleReferences(ArrayList<ArrayList<Tuple>> tupleReferences) {
        this.tupleReferences = tupleReferences;
    }

    public ArrayList<Object> getIndex() {
        return index;
    }

    public void setIndex(ArrayList<Object> index) {
        this.index = index;
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
        
        if(!index.contains(colValue))
            insertionSort(index, colValue);
        
        ArrayList<Tuple> tupleRefrencesPerIndex = tupleReferences.get(index.indexOf(colValue));
        
        if(tupleRefrencesPerIndex == null)
            tupleRefrencesPerIndex = new ArrayList<>(); 
        
        tupleRefrencesPerIndex.add(tuple);

    }
    
    
    /**
     * Inserts a value in an array using insertion sort
     * @param array is the array that the value will be inserted in
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

}
