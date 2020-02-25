package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldType;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> countMap;
    private TupleDesc AggDesc;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.countMap = new HashMap<Field, Integer>();
    }


    private TupleDesc createFlowTupleDesc(Tuple tup){
        if (tup == null) return null;

        TupleDesc tempDesc = tup.getTupleDesc();
        int length = 2;
        
        if (gbfield == Aggregator.NO_GROUPING) length = 1;
    
        Type[] tempTypes = new Type[length];
        String[] tempNames = new String[length];

        if (gbfield == Aggregator.NO_GROUPING) {
            tempNames[0] = tempDesc.getFieldName(afield);
            tempTypes[0] = Type.INT_TYPE;
        }else{
            tempNames[0] = tempDesc.getFieldName(gbfield);
            tempTypes[0] = Type.INT_TYPE;

            tempNames[1] = tempDesc.getFieldName(afield);
            tempTypes[1] = Type.INT_TYPE;
        }

        return new TupleDesc(tempTypes, tempNames);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (tup == null) return;
        Field tempgbf = null;
        Field tempaf = tup.getField(afield);

        if (gbfield == Aggregator.NO_GROUPING){
            tempgbf = new StringField("null", 4);
        }else{
            tempgbf = tup.getField(gbfield);
        }
        
        if (countMap.get(tempgbf) == null){
            if (AggDesc == null) AggDesc = createFlowTupleDesc(tup);
            countMap.put(tempgbf, 1);
            return;
        } else {
            Integer countAvg = countMap.get(tempgbf);
            countAvg++;
            countMap.put(tempgbf, countAvg);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        ArrayList<Tuple> tempList = new ArrayList<Tuple>();
        for (Map.Entry<Field, Integer> entry : countMap.entrySet()) {
            Tuple tempTup = new Tuple(AggDesc);
            Field gField = null;
            int i = 0;
            if (gbfield != Aggregator.NO_GROUPING) {
                gField = entry.getKey();
                tempTup.setField(i++, gField);
            }

            tempTup.setField(i, new IntField(entry.getValue()));

            tempList.add(tempTup);
        }

        return new TupleIterator(AggDesc, tempList);
    }

}
