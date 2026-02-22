package org.karane

import org.apache.spark.{SparkConf, SparkContext}

object Ex04_WordCountJob {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("04 - Word Count")
    val sc = new SparkContext(conf)

    try {
      println()
      println("=" * 70)
      println("4. WORD COUNT")
      println("=" * 70)

      val wordCounts = sc.textFile("/data/words.txt")
        .flatMap(_.toLowerCase.split("\\s+"))
        .map(word => (word, 1))
        .reduceByKey(_ + _)

      // Sort by count descending — swap the pair then sort by key
      val sortedByCount = wordCounts
        .map { case (word, count) => (count, word) }
        .sortByKey(ascending = false)

      println("Word counts (sorted by frequency):")
      sortedByCount.collect().foreach { case (count, word) =>
        println(f"  $word%-15s -> $count")
      }
    } finally {
      sc.stop()
    }
  }
}
