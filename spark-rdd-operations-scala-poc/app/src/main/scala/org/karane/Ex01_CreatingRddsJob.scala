package org.karane

import org.apache.spark.{SparkConf, SparkContext}

object Ex01_CreatingRddsJob {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("01 - Creating RDDs")
    val sc = new SparkContext(conf)

    try {
      println()
      println("=" * 70)
      println("1. CREATING RDDs")
      println("=" * 70)

      // From a Scala collection (parallelize)
      val fromList = sc.parallelize(1 to 10)
      println(s"RDD from list: ${fromList.collect().mkString("[", ", ", "]")}")
      println(s"Partitions: ${fromList.getNumPartitions}")

      // From a collection with explicit partition count
      val withPartitions = sc.parallelize(1 to 6, 3)
      val glomStr = withPartitions.glom().collect()
        .map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]")
      println(s"RDD with 3 partitions — glom: $glomStr")

      // From a text file
      val fromFile = sc.textFile("/data/words.txt")
      println(s"RDD from file (first 3 lines): ${fromFile.take(3).mkString("[", ", ", "]")}")
      println(s"Total lines: ${fromFile.count()}")
    } finally {
      sc.stop()
    }
  }
}
