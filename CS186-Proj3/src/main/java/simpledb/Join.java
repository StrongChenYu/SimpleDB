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
    private Tuple[] leftBuffer;
    private Tuple[] rightBuffer;
    private ArrayList<Tuple> tempTps;
    private HashMap<Integer, Integer> map;

    //131072 is the default buffer of mysql join operation
    public static final int BLOCKMEMORY = 131072 * 5;

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
        String field = child1.getTupleDesc().getFieldName(p.getField1());
        return field;
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        String field = child1.getTupleDesc().getFieldName(p.getField1());
        return field;
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
        tpIter = sortMergeAndBlockNestLoop();
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
        tpIter = sortMergeAndBlockNestLoop();
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

    /*执行 sort-merge 算法 link(https://en.wikipedia.org/wiki/Sort-merge_join)
    * join算法有很多种，我目前想到了三种解法
    * 1. 用hashmap缓存一张表。但是缺点是只适合于外表内容很少的情况下
    * 2. 用数组缓存，使用BNL算法，缺点是速度还是满，第一个query用了0.3s，第二个query用了2s，第三个query用了423.04s，第三个速度太慢
    * 3. sort-merge 算法 排序算法决定整个程序运行速度下限，刚开始使用冒泡排序，第二个query用了140多秒，第三个就更不用说了
    *    之后使用java内置的sort速度明显提高，第一个0.40s，第二个2.30s，第三个6.28s
    */

    /*
    * 只使用一边进行缓存
    */
    @Deprecated
    private Iterator<Tuple> SingleBlockNestLoop() throws DbException, TransactionAbortedException {
        tempTps = new ArrayList<Tuple>();

        //只使用右缓存
        int rightBufferSize = BLOCKMEMORY / child2.getTupleDesc().getSize();
        int rbSize = 0;
        rightBuffer = new Tuple[rightBufferSize];

        while (child2.hasNext()) {

            //读右缓存
            while (rbSize < rightBufferSize && child2.hasNext()) {
                rightBuffer[rbSize++] = child2.next();
            }

            while (child1.hasNext()) {
                Tuple ltp = child1.next();
                for (int i = 0; i < rbSize; i++) {
                    //执行merge
                    Tuple rtp = rightBuffer[i];
                    if (p.filter(ltp, rtp)) tempTps.add(mergeTuple(ltp, rtp));
                }
            }
            child1.rewind();
            rbSize = 0;
        }

        return tempTps.iterator();
    }

    /*
    * 用双边缓存DoubleBlock进行优化IO，但是使用Nestloop算法进行merge
    * deprecated表示该方法不太适合这里使用，性能太低
    */
    @Deprecated
    private void nestLoopMerge(int leftSize, int rightSize) {
        int left = 0;
        int right = 0;
        while (left < leftSize) {
            Tuple ltup = leftBuffer[left];
            while (right < rightSize) {
                Tuple rtup = rightBuffer[right];
                if (p.filter(ltup, rtup)) tempTps.add(mergeTuple(ltup, rtup));
                right++;
            }
            right = 0;
            left++;
        }
    }

    /*
    * sort-merge + blockNl方法
    * 左表和右表同时缓存，这部分使用的时blockNl方法
    * 当左右缓存都满的时候，执行sort-merge方法
    * 算法的时间上限取决于排序算法的程度，因为好的排序算法的时间复杂度可以下降到n*log(n)
    */
    private Iterator<Tuple> sortMergeAndBlockNestLoop() throws TransactionAbortedException, DbException {
        int tpSize1 = child1.getTupleDesc().numFields();
        int tpSize2 = child2.getTupleDesc().numFields();
        tempTps = new ArrayList<Tuple>();

        //use sorted-merge algorithm
        int leftBufferSize = BLOCKMEMORY / child1.getTupleDesc().getSize();
        int rightBufferSize = BLOCKMEMORY / child2.getTupleDesc().getSize();

        leftBuffer = new Tuple[leftBufferSize];
        rightBuffer = new Tuple[rightBufferSize];

        int leftIndex = 0;
        int rightIndex = 0;

        //先将数据读取到buffer里面
        while (child1.hasNext()){
            Tuple tp1 = child1.next();
            leftBuffer[leftIndex++] = tp1;

            //左缓存没有读满就一直读
            if (leftIndex < leftBufferSize) continue;

            //左缓存读满，读右表，直至读满
            while (child2.hasNext()){
                Tuple tp2 = child2.next();
                rightBuffer[rightIndex++] = tp2;

                //右缓存没有读满就一直读
                if (rightIndex < rightBufferSize) continue;

                //右缓存读满 && 左缓存读满

                //sortMerge表示使用了sort-merge算法
                sortMerge(leftIndex, rightIndex);

                //nestloopMerge表示使用了nest loop merge算法
                //nestLoopMerge(leftIndex, rightIndex);

                //重置右缓存
                rightIndex = 0;
            }

            //处理剩余的右缓存（右缓存没满 && 左缓存已满）
            if (rightIndex < rightBufferSize) {

                sortMerge(leftIndex, rightIndex);
                //nestLoopMerge(leftIndex, rightIndex);

                //重置右缓存
                rightIndex = 0;
            }

            //reset buffer
            leftIndex = 0;
            child2.rewind();
        }

        //左缓存没满
        if (leftIndex != 0) {

            //读右表，直至读满
            while (child2.hasNext()){
                Tuple tp2 = child2.next();
                rightBuffer[rightIndex++] = tp2;

                //右缓存没有读满就一直读
                if (rightIndex < rightBufferSize) continue;

                //右缓存读满 && 左缓存没满

                sortMerge(leftIndex, rightIndex);
                //nestLoopMerge(leftIndex, rightIndex);

                //重置右缓存
                rightIndex = 0;
            }

            //（右缓存没满 && 左缓存没满）
            if (rightIndex < rightBufferSize) {

                sortMerge(leftIndex, rightIndex);
                //nestLoopMerge(leftIndex, rightIndex);

                //重置右缓存
                rightIndex = 0;
            }

        }

        return tempTps.iterator();
    }

    private void sortMerge(int leftSize, int rightSize) {
        if (leftSize == 0 || rightSize == 0 ) return;

        //EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;
        switch (p.getOperator()){
            case EQUALS:
                handleEqual(leftSize, rightSize);
                break;
            case GREATER_THAN:
            case GREATER_THAN_OR_EQ:
                handleGreaterThan(leftSize, rightSize);
                break;
            case LESS_THAN:
            case LESS_THAN_OR_EQ:
                handleLessThan(leftSize, rightSize);
                break;
        }

    }

    private void handleLessThan(int leftSize, int rightSize) {
        sort(leftBuffer, leftSize, p.getField1(), false);
        sort(rightBuffer, rightSize, p.getField2(), false);

        int left = 0;
        int right = 0;

        while (left < leftSize && right < rightSize) {
            Tuple ltp = leftBuffer[left];
            Tuple rtp = rightBuffer[right];

            if (p.filter(ltp, rtp)){
                for (int i = right; i < rightSize; i++) {
                    Tuple rtpTemp = rightBuffer[i];
                    Tuple tp = mergeTuple(ltp, rtpTemp);
                    tempTps.add(tp);
                }
                left++;
            } else {
                right++;
            }
        }
    }

    private void handleGreaterThan(int leftSize, int rightSize) {
        sort(leftBuffer, leftSize, p.getField1(), true);
        sort(rightBuffer, rightSize, p.getField2(), true);

        int left = 0;
        int right = 0;

        while (left < leftSize && right < rightSize) {
            Tuple ltp = leftBuffer[left];
            Tuple rtp = rightBuffer[right];

            if (p.filter(ltp, rtp)){
                //将比他小的都合并在一起
                for (int i = right; i < rightSize; i++) {
                    Tuple rtpTemp = rightBuffer[i];
                    Tuple tp = mergeTuple(ltp, rtpTemp);
                    tempTps.add(tp);
                }
                left++;
            } else {
                right++;
            }
        }
    }

    private void handleEqual(int leftSize, int rightSize) {
        sort(leftBuffer, leftSize, p.getField1(), false);
        sort(rightBuffer, rightSize, p.getField2(), false);

        int left = 0;
        int right = 0;

        JoinPredicate greatThan = new JoinPredicate(p.getField1(), Predicate.Op.GREATER_THAN, p.getField2());

        boolean equalFlag = true;
        int leftFlag = 0;

        while (left < leftSize && right < rightSize) {
            Tuple ltp = leftBuffer[left];
            Tuple rtp = rightBuffer[right];

            if (p.filter(ltp, rtp)){
                if (equalFlag) {
                    leftFlag = left;
                    equalFlag = !equalFlag;
                }
                Tuple tp = mergeTuple(ltp, rtp);
                tempTps.add(tp);
                left++;

                if (right < rightSize && left >= leftSize) {
                    right++;
                    left = leftFlag;
                    equalFlag = !equalFlag;
                }

            } else if (greatThan.filter(ltp, rtp)){
                right++;
                left = leftFlag;
                equalFlag = !equalFlag;
            } else {
                left++;
            }
        }
    }

    private void sort(Tuple[] buffer, int length, int field, boolean reverse) {
        CompareTp co = new CompareTp(reverse, field);
        //arrays.sort内置使用归并加快速排序算法，时间复杂度大概为n*log(n)
        Arrays.sort(buffer, 0, length, co);
    }




    class CompareTp implements Comparator<Tuple>{

        private JoinPredicate cop;

        public CompareTp(boolean reverse, int field){
            super();
            if (reverse) {
                cop = new JoinPredicate(field, Predicate.Op.LESS_THAN, field);
            } else {
                cop = new JoinPredicate(field, Predicate.Op.GREATER_THAN, field);
            }
        }

        @Override
        public int compare(Tuple t1, Tuple t2){
            //t1>t2
            if (cop.filter(t1, t2)){
                return 1;
            } else if (cop.filter(t2, t1)){
                return -1;
            } else {
                return 0;
            }
        }
    }

    private Tuple mergeTuple(Tuple tp1, Tuple tp2) {
        int tpSize1 = tp1.getTupleDesc().numFields();
        int tpSize2 = tp2.getTupleDesc().numFields();

        Tuple tempTp = new Tuple(getTupleDesc());
        int i = 0;
        for (; i < tpSize1; i++){
            tempTp.setField(i, tp1.getField(i));
        }

        for (; i < tpSize2 + tpSize1 ; i++){
            tempTp.setField(i, tp2.getField(i-tpSize1));
        }

        return tempTp;
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
