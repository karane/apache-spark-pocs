package org.karane

import org.apache.spark.{SparkConf, SparkContext}

object Ex09_LogAnalysisJob {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("09 - Log Analysis")
    val sc = new SparkContext(conf)

    try {
      println()
      println("=" * 70)
      println("9. REAL-WORLD EXAMPLE — Log Analysis")
      println("=" * 70)

      val logs = sc.textFile("/data/logs.txt")

      // Count by log level
      val logLevelCounts = logs
        .map(_.split("\\s+"))
        .filter(_.length >= 4)
        .map(parts => (parts(2), 1))
        .reduceByKey(_ + _)
      println("Log level counts:")
      logLevelCounts.collect().foreach { case (level, count) =>
        println(f"  $level%-8s -> $count")
      }

      // Extract ERROR messages
      val errors = logs.filter(_.contains("ERROR"))
      println("\nERROR messages:")
      errors.collect().foreach(e => println(s"  $e"))

      // Count errors per service
      val errorsByService = errors
        .map(_.split("\\s+"))
        .filter(_.length >= 4)
        .map(parts => (parts(3), 1))
        .reduceByKey(_ + _)
        .sortByKey()
      println("\nErrors by service:")
      errorsByService.collect().foreach { case (service, count) =>
        println(f"  $service%-25s -> $count")
      }

      // Count events per hour
      val eventsPerHour = logs
        .map(line => (line.substring(11, 13) + ":00", 1))
        .reduceByKey(_ + _)
        .sortByKey()
      println("\nEvents per hour:")
      eventsPerHour.collect().foreach { case (hour, count) =>
        println(s"  $hour -> $count")
      }
    } finally {
      sc.stop()
    }
  }
}
