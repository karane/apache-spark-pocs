package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SimpleJob {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Simple Java Spark Job");
        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            banner("1. CREATING AN RDD");
            demoCreatingRdd(sc);

            banner("2. TRANSFORMATIONS — map, filter, flatMap");
            demoTransformations(sc);

            banner("3. ACTIONS — count, reduce, collect, take");
            demoActions(sc);

        } finally {
            sc.close();
        }
    }

    // ========================================================================
    // 1. Creating an RDD
    // ========================================================================
    private static void demoCreatingRdd(JavaSparkContext sc) {
        // Parallelize a Java collection into an RDD
        JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        System.out.println("RDD elements: " + numbers.collect());
        System.out.println("Number of partitions: " + numbers.getNumPartitions());

        // Parallelize with an explicit partition count
        JavaRDD<Integer> partitioned = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 3);
        System.out.println("RDD with 3 partitions — glom: " + partitioned.glom().collect());
    }

    // ========================================================================
    // 2. Transformations
    // ========================================================================
    private static void demoTransformations(JavaSparkContext sc) {
        JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        // map — square each number
        JavaRDD<Integer> squared = numbers.map(n -> n * n);
        System.out.println("map (squared): " + squared.collect());

        // filter — keep only even numbers
        JavaRDD<Integer> evens = numbers.filter(n -> n % 2 == 0);
        System.out.println("filter (evens): " + evens.collect());

        // flatMap — split sentences into words
        JavaRDD<String> sentences = sc.parallelize(Arrays.asList(
                "hello world", "apache spark", "simple java job"));
        JavaRDD<String> words = sentences.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        System.out.println("flatMap (words): " + words.collect());
    }

    // ========================================================================
    // 3. Actions
    // ========================================================================
    private static void demoActions(JavaSparkContext sc) {
        JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(3, 1, 4, 1, 5, 9, 2, 6, 5, 3));

        // count — number of elements
        System.out.println("count: " + numbers.count());

        // first — first element
        System.out.println("first: " + numbers.first());

        // take — first N elements
        System.out.println("take(3): " + numbers.take(3));

        // collect — bring all elements to the driver
        List<Integer> all = numbers.collect();
        System.out.println("collect: " + all);

        // reduce — sum all elements
        int sum = numbers.reduce(Integer::sum);
        System.out.println("reduce (sum): " + sum);

        // reduce — find maximum
        int max = numbers.reduce(Integer::max);
        System.out.println("reduce (max): " + max);
    }

    // ========================================================================
    // Helper
    // ========================================================================
    private static void banner(String title) {
        System.out.println("\n" + "=".repeat(70));
        System.out.println(title);
        System.out.println("=".repeat(70));
    }
}
