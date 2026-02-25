package org.karane

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Ex07_PartitioningJob {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("07 - Partitioning")
    val sc = new SparkContext(conf)

    try {
      println()
      println("=" * 70)
      println("7. PARTITIONING — repartition, coalesce, getNumPartitions, glom, HashPartitioner")
      println("=" * 70)

      val numbers = sc.parallelize(1 to 12, 4)

      // getNumPartitions
      println(s"Initial partitions: ${numbers.getNumPartitions}")

      // glom — see data distribution across partitions
      val glomStr = numbers.glom().collect()
        .map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]")
      println(s"glom (data per partition): $glomStr")

      // repartition — increase partitions (causes shuffle)
      val morePartitions = numbers.repartition(6)
      println(s"After repartition(6): ${morePartitions.getNumPartitions} partitions")
      val moreGlom = morePartitions.glom().collect()
        .map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]")
      println(s"  glom: $moreGlom")

      // coalesce — decrease partitions (avoids full shuffle)
      val fewerPartitions = numbers.coalesce(2)
      println(s"After coalesce(2): ${fewerPartitions.getNumPartitions} partitions")
      val fewerGlom = fewerPartitions.glom().collect()
        .map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]")
      println(s"  glom: $fewerGlom")

      // Pair RDD partitioning with partitionBy
      val pairs = sc.parallelize(Seq(
        ("a", 1), ("b", 2), ("a", 3),
        ("c", 4), ("b", 5), ("c", 6)
      ))
      val hashPartitioned = pairs.partitionBy(new HashPartitioner(3))
      val hashGlom = hashPartitioned.glom().collect()
        .map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]")
      println(s"HashPartitioner(3) — glom: $hashGlom")
    } finally {
      sc.stop()
    }
  }
}
