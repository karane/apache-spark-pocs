package org.karane

import org.apache.spark.{SparkConf, SparkContext}

object Ex02_BasicTransformationsJob {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("02 - Basic Transformations")
    val sc = new SparkContext(conf)

    try {
      println()
      println("=" * 70)
      println("2. TRANSFORMATIONS — map, flatMap, filter, mapPartitions, mapPartitionsWithIndex")
      println("=" * 70)

      val numbers = sc.parallelize(1 to 10)

      // map — square each number
      val squared = numbers.map(n => n * n)
      println(s"map (squared): ${squared.collect().mkString("[", ", ", "]")}")

      // filter — keep only even numbers
      val evens = numbers.filter(_ % 2 == 0)
      println(s"filter (evens): ${evens.collect().mkString("[", ", ", "]")}")

      // flatMap — split sentences into words
      val sentences = sc.parallelize(Seq("hello world", "apache spark", "rdd operations"))
      val words = sentences.flatMap(_.split(" "))
      println(s"flatMap (words): ${words.collect().mkString("[", ", ", "]")}")

      // mapPartitions — process entire partitions at once (more efficient)
      val partitionSums = numbers.repartition(3).mapPartitions { iter =>
        val sum = iter.sum
        Iterator(sum)
      }
      println(s"mapPartitions (partition sums): ${partitionSums.collect().mkString("[", ", ", "]")}")

      // mapPartitionsWithIndex — includes partition index
      val withIndex = numbers.repartition(3).mapPartitionsWithIndex { (idx, iter) =>
        val elements = iter.mkString(", ")
        Iterator(s"Partition $idx: [$elements]")
      }
      println("mapPartitionsWithIndex:")
      withIndex.collect().foreach(s => println(s"  $s"))
    } finally {
      sc.stop()
    }
  }
}
