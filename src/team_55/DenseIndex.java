package team_55;

import java.util.ArrayList;

public class DenseIndex {
    
    private ArrayList<Object> index;                            // values of the column that the index is built on, should be sorted
    private ArrayList<ArrayList<Tuple>> tupleReferences;         /* sorted according to index Array such that each value in index array corresponds to
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
    
    public void insertTupleRefernces(int colValue) {

    }
    
    private void insertionSort(ArrayList<Object> array, Object value) {
        
    }

}
