package org.rhcalero.bigdata.module1.addnumbersspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * AddNumbers:
 * <p>
 * Sum a list of numbers with Spark from file
 * </p>
 * 
 * @author Hidalgo Calero, R.
 * @since Oct 12, 2016
 */
public class AddNumbersLambdaFile {

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
        SparkConf conf = new SparkConf().setAppName("AddNumbers Lambda File");

        // STEP 2: Create a Java Spark Context
        JavaSparkContext context = new JavaSparkContext(conf);

        // STEP 3: Get input values
        JavaRDD<String> lines = context.textFile(args[0]);

        // STEP 4: Create a Java RDD from lines and convert to Integer
        JavaRDD<Integer> data = lines.map(s -> valueOf(s));

        long initTime = System.currentTimeMillis();

        // STEP 5: Compute the sum
        int sum = data.reduce((integer, integer2) -> sum(integer, integer2));

        long computigTime = System.currentTimeMillis() - initTime;

        // STEP 6: Print the sum
        System.out.println("Total sum: " + sum);
        System.out.println("Computing time: " + computigTime);

        // STEP 7: Stop the spark context
        context.stop();
    }

    /**
     * 
     * Method valueOf.
     * <p>
     * Return Integer value from a String
     * </p>
     * 
     * @param v String value
     * @return Integer value
     */
    private static Integer valueOf(String v) {
        return Integer.valueOf(v);
    }

    /**
     * 
     * Method sum.
     * <p>
     * Return the sum of two integer numbers
     * </p>
     * 
     * @param number1 First number
     * @param number2 Second number
     * @return The sum
     */
    private static Integer sum(Integer number1, Integer number2) {
        return number1 + number2;
    }

}
