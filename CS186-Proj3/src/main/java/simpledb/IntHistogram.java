package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] histogram;
    private double gap_w;
    private int min;
    private int max;
    private double ntups;
    private double min_right;
    private double min_left;
    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        //gap表示一个gap里面存的数的范围数，比如1-10，gap为10，表示1，2，3，...，10共有gap个数
        this.gap_w = Double.valueOf(max - min + 1) / buckets;
        this.histogram = new int[buckets];
        this.min = min;
        this.max = max;
        this.min_right = min + gap_w - 1;
        this.min_left = min;
        this.ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int bucket = (int) Math.floor((v - min) / gap_w);
        histogram[bucket]++;
        ntups = ntups + 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        double selectivity = 0.0;

        switch (op) {
            case EQUALS:
                selectivity = estimateEqueals(v);
                break;
            case NOT_EQUALS:
                selectivity = 1 - estimateEqueals(v);
                break;
            case GREATER_THAN:
                selectivity = estimateGreaterThan(v);
                break;
            case GREATER_THAN_OR_EQ:
                selectivity = estimateGreaterThan(v) + estimateEqueals(v);
                break;
            case LESS_THAN:
                selectivity = estimateLessThan(v);
                break;
            case LESS_THAN_OR_EQ:
                selectivity = estimateLessThan(v) + estimateEqueals(v);
                break;
            default:
                throw new IllegalStateException("impossible to reach here");
        }

        return selectivity;
    }


    private double estimateEqueals(int v) {
        if (v > max || v < min) return 0.0;

        double selectivity = 0.0;
        double h = histogram[(int) Math.floor((v - min)/ gap_w)];
        selectivity = h / gap_w / ntups;

        return selectivity;
    }

    private double estimateGreaterThan(int v) {
        if (v >= max) return 0.0;
        if (v < min) return 1.0;

        double selectivity = 0.0;
        int bucket = (int) Math.floor((v - min) / gap_w);
        double b_f = histogram[bucket] / ntups;
        double b_right = min_right + gap_w * bucket;
        double b_part = (b_right - v) / gap_w;

        for (int i = bucket + 1; i < histogram.length; i++) {
            selectivity += histogram[i] / ntups;
        }

        selectivity += b_f * b_part;
        return selectivity;
    }

    private double estimateLessThan(int v) {
        if (v > max) return 1.0;
        if (v <= min) return 0.0;

        double selectivity = 0.0;
        int bucket = (int) Math.floor((v - min) / gap_w);
        double b_f = histogram[bucket] / ntups;
        double b_left = min_left + gap_w * bucket;
        double b_part = (v - b_left) / gap_w;

        for (int i = 0; i < bucket; i++) {
            selectivity += histogram[i] / ntups;
        }

        selectivity += b_f * b_part;

        return selectivity;
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }

    public static void main(String[] args) {
        int max = 10;
        int min = 6;
        int buckets = 4;
        double gap_w = Double.valueOf(max - min + 1) / (buckets);
        int []histogram = new int[buckets];
        System.out.println(histogram[1]);
    }
}
