package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 *
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    private static final int BUCKET = 10;

    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        //这里是要遍历整个table，对于每一个table计算stats
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    private int tableid;
    private int iocostperpage;
    private int ntups;
    private HeapFile table;
    private HashMap<Integer, Object> hisArray;
    private HashMap<Integer, Integer[]> maxAndmin;
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableid = tableid;
        this.iocostperpage = ioCostPerPage;
        this.ntups = 0;
        this.hisArray = new HashMap<Integer, Object>();
        this.maxAndmin = new HashMap<Integer, Integer[]>();
        //初始化直方图，用于统计数据用
        this.initHistogram();
    }

    private void initHistogram() {
        Transaction t = new Transaction();
        DbIterator iter = new SeqScan(t.getId(), tableid);

        try {
            iter.open();
            //第一次循环找出int的最大最小值，统计tuple数量，并建立histogram表
            while (iter.hasNext()) {
                //统计总的tuple数量
                ntups++;

                Tuple tup = iter.next();
                TupleDesc tupDes = tup.getTupleDesc();

                for (int i = 0; i < tupDes.numFields(); i++) {
                    Type type = tupDes.getFieldType(i);

                    if (type.equals(Type.INT_TYPE)) {
                        IntField intf = (IntField) tup.getField(i);
                        Integer value = intf.getValue();

                        if (maxAndmin.containsKey(i)) {
                            Integer[] values = maxAndmin.get(i);

                            //更新最大值
                            if (value > values[0]) values[0] = value;

                            //更新最小值
                            if (value < values[1]) values[1] = value;

                        } else {
                            Integer[] values = new Integer[]{value, value};
                            maxAndmin.put(i, values);
                        }
                    }
                }
            }


            //第二次循环添加数据填充histogram
            iter.rewind();
            while (iter.hasNext()) {
                Tuple tup = iter.next();
                TupleDesc tupDes = tup.getTupleDesc();

                for (int i = 0; i < tupDes.numFields(); i++) {
                    Type type = tupDes.getFieldType(i);

                    switch (type) {
                        case INT_TYPE:
                            IntField intf = (IntField) tup.getField(i);
                            int intv = intf.getValue();

                            if (hisArray.containsKey(i)) {
                                IntHistogram intHis = (IntHistogram) hisArray.get(i);
                                intHis.addValue(intv);
                            } else {
                                int max = maxAndmin.get(i)[0];
                                int min = maxAndmin.get(i)[1];
                                hisArray.put(i, new IntHistogram(BUCKET, min, max));
                            }
                            break;
                        case STRING_TYPE:

                            StringField sf = (StringField) tup.getField(i);
                            String sv = sf.getValue();

                            if (hisArray.containsKey(i)) {
                                StringHistogram sHis = (StringHistogram) hisArray.get(i);
                                sHis.addValue(sv);
                            } else {
                                hisArray.put(i, new StringHistogram(BUCKET));
                            }
                            break;
                        default:
                    }

                }
            }

            iter.close();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } catch (DbException e) {
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     *
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    // 假设在bufferpool中预先没有page，硬盘一次只读取一个完整的页，不管它是否满tuple
    public double estimateScanCost() {
        // some code goes here
        HeapFile table = (HeapFile)Database.getCatalog().getDbFile(tableid);
        //页数 x 读取每一页的iocost，难道不是这样子吗？
        return table.numPages() * iocostperpage;
        //return 0;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {

        return (int) Math.ceil(totalTuples() * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        double selectivity = 0.0;

        Type type = constant.getType();

        switch (type) {
            case STRING_TYPE:
                String sv = ((StringField)constant).getValue();
                StringHistogram sHis = (StringHistogram) hisArray.get(field);
                selectivity = sHis.estimateSelectivity(op, sv);
                break;
            case INT_TYPE:
                int iv = ((IntField)constant).getValue();
                IntHistogram iHis = (IntHistogram) hisArray.get(field);
                iHis.estimateSelectivity(op, iv);
                selectivity = iHis.estimateSelectivity(op, iv);
                break;
            default:
                //运行不到这里
        }
        return selectivity;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return ntups;
    }

}
