package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate p;
    private DbIterator child1;
    private DbIterator child2;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        SeqScan scan = (SeqScan)child1;
        return scan.getTableName();
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        SeqScan scan = (SeqScan)child2;
        return scan.getAlias();
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc td1 = child1.getTupleDesc();
        TupleDesc td2 = child2.getTupleDesc();
        return TupleDesc.merge(td1, td2);
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child1.open();
        child2.open();
        tpIter = getAllFetchNext();
    }

    public void close() {
        // some code goes here
        child1.close();
        child2.close();
        super.close();
        tpIter = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
        tpIter = getAllFetchNext();
    }
    private Iterator<Tuple> tpIter = null;
    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (tpIter == null) return null;
        Tuple tp = null;
        if (tpIter.hasNext()){
            tp = tpIter.next();
        }
        return tp;
    }

    private Iterator<Tuple> getAllFetchNext() throws TransactionAbortedException, DbException {
        int tpSize1 = child1.getTupleDesc().numFields();
        int tpSize2 = child2.getTupleDesc().numFields();
        ArrayList<Tuple> tempTps = new ArrayList<Tuple>();
        
        while (child1.hasNext()){
            Tuple tp1 = child1.next();
            while (child2.hasNext()){
                Tuple tp2 = child2.next();
                if (p.filter(tp1, tp2)){
                    Tuple tempTp = new Tuple(getTupleDesc());
                    int i = 0;
                    for (; i < tpSize1; i++){
                        tempTp.setField(i, tp1.getField(i));
                    }

                    for (; i < tpSize2 + tpSize1 ; i++){
                        tempTp.setField(i, tp2.getField(i-tpSize1));
                    }
                    tempTps.add(tempTp);
                }
            }
            child2.rewind();
        }
        return tempTps.iterator();
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{ child1, child2 };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        child1 = children[0];
        child2 = children[1];
    }

}
