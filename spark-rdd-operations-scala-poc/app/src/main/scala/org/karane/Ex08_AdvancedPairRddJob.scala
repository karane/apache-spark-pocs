package org.karane

import org.apache.spark.{SparkConf, SparkContext}

object Ex08_AdvancedPairRddJob {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("08 - Advanced Pair RDD")
    val sc = new SparkContext(conf)

    try {
      println()
      println("=" * 70)
      println("8. ADVANCED PAIR RDD — combineByKey, aggregateByKey, join, cogroup")
      println("=" * 70)

      // combineByKey — compute average per key
      val scores = sc.parallelize(Seq(
        ("Alice", 85), ("Bob", 72),
        ("Alice", 92), ("Bob", 88),
        ("Alice", 78), ("Charlie", 95),
        ("Bob", 65), ("Charlie", 90)
      ))

      // combineByKey to calculate average: (totalScore, count)
      val combined = scores.combineByKey(
        (score: Int) => (score, 1),
        (acc: (Int, Int), score: Int) => (acc._1 + score, acc._2 + 1),
        (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
      )
      val averages = combined.mapValues { case (total, count) => total.toDouble / count }
      println(s"combineByKey (averages): ${averages.collect().mkString("[", ", ", "]")}")

      // aggregateByKey — same result, different API
      val aggregated = scores.aggregateByKey((0, 0))(
        (acc, score) => (acc._1 + score, acc._2 + 1),
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
      )
      val aggAvg = aggregated.mapValues { case (total, count) => total.toDouble / count }
      println(s"aggregateByKey: ${aggAvg.collect().mkString("[", ", ", "]")}")

      // join
      val names = sc.parallelize(Seq(("1", "Alice"), ("2", "Bob"), ("3", "Charlie")))
      val departments = sc.parallelize(Seq(("1", "Engineering"), ("2", "Marketing"), ("4", "Sales")))

      // inner join
      println(s"join (inner): ${names.join(departments).collect().mkString("[", ", ", "]")}")

      // left outer join
      println(s"leftOuterJoin: ${names.leftOuterJoin(departments).collect().mkString("[", ", ", "]")}")

      // right outer join
      println(s"rightOuterJoin: ${names.rightOuterJoin(departments).collect().mkString("[", ", ", "]")}")

      // full outer join
      println(s"fullOuterJoin: ${names.fullOuterJoin(departments).collect().mkString("[", ", ", "]")}")

      // cogroup — group data from multiple RDDs by key
      val cogrouped = names.cogroup(departments)
      println(s"cogroup: ${cogrouped.collect().mkString("[", ", ", "]")}")
    } finally {
      sc.stop()
    }
  }
}
