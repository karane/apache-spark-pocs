package org.karane

import org.apache.spark.{SparkConf, SparkContext}

object Ex05_SetOperationsJob {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("05 - Set Operations")
    val sc = new SparkContext(conf)

    try {
      println()
      println("=" * 70)
      println("5. SET OPERATIONS")
      println("=" * 70)

      val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5))
      val rdd2 = sc.parallelize(Seq(4, 5, 6, 7, 8))

      // union
      val unioned = rdd1.union(rdd2)
      println(s"union: ${unioned.collect().mkString("[", ", ", "]")}")

      // intersection
      val intersected = rdd1.intersection(rdd2)
      println(s"intersection: ${intersected.collect().mkString("[", ", ", "]")}")

      // subtract
      val subtracted = rdd1.subtract(rdd2)
      println(s"subtract (rdd1 - rdd2): ${subtracted.collect().mkString("[", ", ", "]")}")

      // distinct
      val unique = unioned.distinct()
      println(s"distinct: ${unique.collect().mkString("[", ", ", "]")}")

      // cartesian
      val cartesian = rdd1.cartesian(rdd2)
      println(s"cartesian (first 5): ${cartesian.take(5).mkString("[", ", ", "]")}")
      println(s"cartesian count: ${cartesian.count()}")
    } finally {
      sc.stop()
    }
  }
}
