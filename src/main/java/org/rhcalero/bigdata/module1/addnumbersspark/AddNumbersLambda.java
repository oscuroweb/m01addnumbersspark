package org.rhcalero.bigdata.module1.addnumbersspark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * AddNumbers:
 * <p>
 * Sum a list of numbers with Spark
 * </p>
 * 
 * @author Hidalgo Calero, R.
 * @since Oct 12, 2016
 */
public class AddNumbersLambda {

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

        // STEP 1: Create a SparkCOnf objet
        SparkConf conf = new SparkConf().setAppName("AddNumbers Lambda");

        // STEP 2: Create a Java Spark Context
        JavaSparkContext context = new JavaSparkContext(conf);

        // Create a list of values
        List<Integer> data = Arrays.asList(new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 });

        // STEP 3: Create a Java RDD
        JavaRDD<Integer> distributedData = context.parallelize(data);

        // STEP 4: Compute the sum
        int sum = distributedData.reduce((number1, number2) -> sum(number1, number2));

        // STEP 5: Print the sum
        System.out.println("Total sum: " + sum);

        // STEP 6: Stop the spark context
        context.stop();
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
