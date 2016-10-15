package org.rhcalero.bigdata.module1.addnumbersspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * AddNumbers:
 * <p>
 * Sum a list of numbers with Spark from file
 * </p>
 * 
 * @author Hidalgo Calero, R.
 * @since Oct 12, 2016
 */
public class AddNumbersFile {

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
        SparkConf conf = new SparkConf().setAppName("AddNumbers File");

        // STEP 2: Create a Java Spark Context
        JavaSparkContext context = new JavaSparkContext(conf);

        // STEP 3: Get input values
        JavaRDD<String> lines = context.textFile(args[0]);

        // STEP 4: Create a Java RDD from lines and convert to Integer
        JavaRDD<Integer> data = lines.map(new Function<String, Integer>() {
            public Integer call(String s) throws Exception {
                return Integer.valueOf(s);
            }
        });

        long initTime = System.currentTimeMillis();

        // STEP 5: Compute the sum
        int sum = data.reduce(new Function2<Integer, Integer, Integer>() {

            private static final long serialVersionUID = 746141485041189667L;

            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        long computigTime = System.currentTimeMillis() - initTime;

        // STEP 6: Print the sum
        System.out.println("Total sum: " + sum);
        System.out.println("Computing time: " + computigTime);

        // STEP 7: Stop the spark context
        context.stop();
    }

}
