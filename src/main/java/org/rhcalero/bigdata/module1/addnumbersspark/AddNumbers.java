package org.rhcalero.bigdata.module1.addnumbersspark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * AddNumbers:
 * <p>
 * Sum a list of numbers with Spark
 * </p>
 * 
 * @author Hidalgo Calero, R.
 * @since Oct 12, 2016
 */
public class AddNumbers {

    /**
     * 
     * Method main.
     * <p>
     * Execute AddNumbers program
     * </p>
     * 
     * @param args List of inputs values
     */
    public static void main(String[] args) {

        // STEP 1: Create a SparkConf object
        SparkConf conf = new SparkConf().setAppName("AddNumbers");

        // STEP 2: Create a Java Spark Context
        JavaSparkContext context = new JavaSparkContext(conf);

        // Create a list of values
        List<Integer> data = Arrays.asList(new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 });

        // STEP 3: Create a Java RDD
        // Prepare spark to work in parallel with a set of data
        JavaRDD<Integer> distributedData = context.parallelize(data);

        // STEP 4: Compute the sum
        int sum = distributedData.reduce(new Function2<Integer, Integer, Integer>() {

            private static final long serialVersionUID = 746141485041189667L;

            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        // STEP 5: Print the sum
        System.out.println("Total sum: " + sum);

        // STEP 6: Stop the spark context
        context.stop();
    }

}
