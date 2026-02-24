package org.karane

import org.apache.spark.{SparkConf, SparkContext}

object Ex06_ActionsJob {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("06 - Actions")
    val sc = new SparkContext(conf)

    try {
      println()
      println("=" * 70)
      println("6. ACTIONS ")
      println("=" * 70)

      val numbers = sc.parallelize(Seq(3, 1, 4, 1, 5, 9, 2, 6, 5, 3))

      // collect — return all elements
      println(s"collect: ${numbers.collect().mkString("[", ", ", "]")}")

      // count
      println(s"count: ${numbers.count()}")

      // first
      println(s"first: ${numbers.first()}")

      // take
      println(s"take(3): ${numbers.take(3).mkString("[", ", ", "]")}")

      // takeOrdered — smallest elements
      println(s"takeOrdered(4): ${numbers.takeOrdered(4).mkString("[", ", ", "]")}")

      // top — largest elements
      println(s"top(4): ${numbers.top(4).mkString("[", ", ", "]")}")

      // countByValue
      val counts = numbers.countByValue()
      println(s"countByValue: $counts")

      // reduce — sum all
      val sum = numbers.reduce(_ + _)
      println(s"reduce (sum): $sum")

      // reduce — find max
      val max = numbers.reduce(_ max _)
      println(s"reduce (max): $max")

      // fold — sum with zero value
      val foldSum = numbers.fold(0)(_ + _)
      println(s"fold (sum): $foldSum")

      // aggregate — compute (sum, count) in one pass to get average
      val result = numbers.aggregate((0, 0))(
        (acc, value) => (acc._1 + value, acc._2 + 1),
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
      )
      val average = result._1.toDouble / result._2
      println(s"aggregate (sum=${result._1}, count=${result._2}, avg=$average)")

      // foreach — side effect (prints on executors, may not appear in driver log)
      println("foreach — printing on executors (check worker logs)")
      numbers.foreach(n => println(s"  foreach element: $n"))
    } finally {
      sc.stop()
    }
  }
}
