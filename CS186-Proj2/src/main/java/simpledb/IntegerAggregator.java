package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieType;
    private int afield;
    private Op what;
    private TupleDesc AggDesc;
    private HashMap<Field, Integer> afieldNum;
    private HashMap<Field, Integer> gbFieldNum;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieType = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.afieldNum = new HashMap<>();
        this.gbFieldNum = new HashMap<>();
    }

    private TupleDesc createFlowTupleDesc(Tuple tup){

        TupleDesc tempDesc = tup.getTupleDesc();
        int length = 2;
        
        if (gbfield == Aggregator.NO_GROUPING) length = 1;
    
        Type[] tempTypes = new Type[length];
        String[] tempNames = new String[length];

        if (gbfield == Aggregator.NO_GROUPING) {
            tempNames[0] = tempDesc.getFieldName(afield);
            tempTypes[0] = tempDesc.getFieldType(afield);
        }else{
            tempNames[0] = tempDesc.getFieldName(gbfield);
            tempTypes[0] = gbfieType;

            tempNames[1] = tempDesc.getFieldName(afield);
            tempTypes[1] = tempDesc.getFieldType(afield);
        }

        return new TupleDesc(tempTypes, tempNames);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field tempgbf = null;
        IntField tempaf = (IntField)tup.getField(afield);

        if (gbfield == Aggregator.NO_GROUPING){
            tempgbf = new StringField("null", 4);
        }else{
            tempgbf = tup.getField(gbfield);
        }
        
        if (afieldNum.get(tempgbf) == null){
            if (AggDesc == null) AggDesc = createFlowTupleDesc(tup);
            
            afieldNum.put(tempgbf, tempaf.getValue());
            gbFieldNum.put(tempgbf, 1);

            return;
        } 
        
        switch(what){
            case AVG:
                countField(tempgbf);
                sumField(tempgbf, tempaf);
                break; 
            case SUM:
                sumField(tempgbf, tempaf);
                break;
            case MIN:
                minField(tempgbf, tempaf); 
                break;
            case MAX:
                maxField(tempgbf, tempaf); 
                break;
            case COUNT:
                countField(tempgbf);
                break;
            default:
                throw new IllegalStateException("impossible to reach here");
        }
    }

    private void countField(Field tempgbf){
        Integer countAvg = gbFieldNum.get(tempgbf);
        countAvg++;
        gbFieldNum.put(tempgbf, countAvg);
    }

    private void sumField(Field tempgbf, IntField intf) {
        Integer tempValue = afieldNum.get(tempgbf);
        tempValue += intf.getValue();
        afieldNum.put(tempgbf, tempValue);
    }

    private void minField(Field tempgbf, IntField intf) {
        Integer tempValue = afieldNum.get(tempgbf);
        tempValue = Math.min(tempValue, intf.getValue());
        afieldNum.put(tempgbf, tempValue);
    }

    private void maxField(Field tempgbf, IntField intf) {
        Integer tempValue = afieldNum.get(tempgbf);
        tempValue = Math.max(tempValue, intf.getValue());
        afieldNum.put(tempgbf, tempValue);
    }


    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        ArrayList<Tuple> tempList = new ArrayList<>();
        for (Map.Entry<Field, Integer> entry : afieldNum.entrySet()) {
            Tuple tempTup = new Tuple(AggDesc);
            Field gField = entry.getKey();
            int i = 0;
            if (gbfield != Aggregator.NO_GROUPING) {
                tempTup.setField(i++, gField);
            }
            switch (what){
                case AVG:
                    Integer tempAvg = entry.getValue() / gbFieldNum.get(gField);
                    tempTup.setField(i, new IntField(tempAvg));
                    break;
                case COUNT:
                    tempTup.setField(i, new IntField(gbFieldNum.get(gField)));
                    break;
                default:                    
                    tempTup.setField(i, new IntField(entry.getValue()));
            }
            tempList.add(tempTup);
        }

        return new TupleIterator(AggDesc, tempList);
    }

}
