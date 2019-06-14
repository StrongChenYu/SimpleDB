package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private Aggregator aggre;
    private DbIterator aggreResult;
    private TupleDesc tupDesc;
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop){
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        this.tupDesc = getTupleDesc();

        Type aType = child.getTupleDesc().getFieldType(afield);
        Type gType = null;
        if (gfield != Aggregator.NO_GROUPING) gType = child.getTupleDesc().getFieldType(gfield);

        if (aType == Type.INT_TYPE) {
            aggre = new IntegerAggregator(gfield, gType, afield, aop);
        } else {
            aggre = new StringAggregator(gfield, gType, afield, aop);
        }
        
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        if (gfield == -1) return Aggregator.NO_GROUPING;
        else return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
        if (groupField() == Aggregator.NO_GROUPING) return null;
        return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
    	return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	    return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
            child.open();
            super.open();
            while (child.hasNext()){
                aggre.mergeTupleIntoGroup(child.next());
            }
            aggreResult = aggre.iterator();
            aggreResult.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (aggreResult.hasNext()) return aggreResult.next();
	    else return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        aggreResult.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        if (tupDesc != null) return tupDesc;
        
        Type[] tempType = null;
        String[] tempName = null;
        if (gfield==Aggregator.NO_GROUPING) {
            tempName = new String[1];
            tempType = new Type[1];

            tempName[0] = aop.toString() + "(" + child.getTupleDesc().getFieldName(afield) + ")";
            tempType[0] = child.getTupleDesc().getFieldType(afield);
        } else {
            tempName = new String[2];
            tempType = new Type[2];

            tempName[0] = child.getTupleDesc().getFieldName(gfield);
            tempType[0] = child.getTupleDesc().getFieldType(gfield);

            tempName[1] = aop.toString() + "(" + child.getTupleDesc().getFieldName(afield) + ")";
            tempType[1] = child.getTupleDesc().getFieldType(afield);
        }
        
        return new TupleDesc(tempType, tempName);
    }

    public void close() {
        super.close();
        child.close();
        aggreResult.close();
    }

    @Override
    public DbIterator[] getChildren() {
	    return new DbIterator[] {child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }
    
}
