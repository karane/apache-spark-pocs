package org.karane

import org.apache.spark.{SparkConf, SparkContext}

object Ex03_PairRddOperationsJob {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("03 - Pair RDD Operations")
    val sc = new SparkContext(conf)

    try {
      println()
      println("=" * 70)
      println("3. PAIR RDD OPERATIONS — reduceByKey, groupByKey, sortByKey, mapValues, flatMapValues")
      println("=" * 70)

      val lines = sc.textFile("/data/words.txt")

      val wordPairs = lines
        .flatMap(_.split("\\s+"))
        .map(word => (word, 1))
      println(s"map to pairs (first 5): ${wordPairs.take(5).mkString("[", ", ", "]")}")
      println()

      val wordCounts = wordPairs.reduceByKey(_ + _)
      println(s"reduceByKey (word counts): ${wordCounts.collect().mkString("[", ", ", "]")}")
      println()

      val grouped = wordPairs.groupByKey()
      println(s"groupByKey (first 3): ${grouped.take(3).mkString("[", ", ", "]")}")
      println()

      val sorted = wordCounts.sortByKey()
      println(s"sortByKey (ascending): ${sorted.collect().mkString("[", ", ", "]")}")
      println()

      val sortedDesc = wordCounts.sortByKey(ascending = false)
      println(s"sortByKey (descending): ${sortedDesc.collect().mkString("[", ", ", "]")}")
      println()

      val doubled = wordCounts.mapValues(_ * 2)
      println(s"mapValues (doubled counts): ${doubled.collect().mkString("[", ", ", "]")}")
      println()

      val expanded = wordCounts.flatMapValues(v => Seq(v, v * 10))
      println(s"flatMapValues (first 5): ${expanded.take(5).mkString("[", ", ", "]")}")
      println()

      println(s"keys: ${wordCounts.keys.collect().mkString("[", ", ", "]")}")
      println(s"values: ${wordCounts.values.collect().mkString("[", ", ", "]")}")
      println()

    } finally {
      sc.stop()
    }
  }
}
