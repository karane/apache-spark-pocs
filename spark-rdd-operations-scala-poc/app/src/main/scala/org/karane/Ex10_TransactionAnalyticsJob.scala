package org.karane

import org.apache.spark.{SparkConf, SparkContext}

object Ex10_TransactionAnalyticsJob {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("10 - Transaction Analytics")
    val sc = new SparkContext(conf)

    try {
      println()
      println("=" * 70)
      println("10. REAL-WORLD EXAMPLE — Transaction Analytics")
      println("=" * 70)

      val raw = sc.textFile("/data/transactions.csv")

      // Skip header and parse
      val header = raw.first()
      val transactions = raw
        .filter(_ != header)
        .map(_.split(","))

      // Total revenue
      val totalRevenue = transactions
        .map(_(3).toDouble)
        .reduce(_ + _)
      println(f"Total revenue: $$$totalRevenue%.2f")

      // Revenue per category
      val revenueByCategory = transactions
        .map(parts => (parts(2), parts(3).toDouble))
        .reduceByKey(_ + _)
      println("\nRevenue by category:")
      revenueByCategory.collect().foreach { case (cat, rev) =>
        println(f"  $cat%-15s -> $$$rev%.2f")
      }

      // Number of purchases per user
      val purchasesPerUser = transactions
        .map(parts => (s"user_${parts(0)}", 1))
        .reduceByKey(_ + _)
        .sortByKey()
      println("\nPurchases per user:")
      purchasesPerUser.collect().foreach { case (user, count) =>
        println(f"  $user%-10s -> $count")
      }

      // Average transaction amount per category using combineByKey
      val categoryTotals = transactions
        .map(parts => (parts(2), parts(3).toDouble))
        .combineByKey(
          (amount: Double) => (amount, 1),
          (acc: (Double, Int), amount: Double) => (acc._1 + amount, acc._2 + 1),
          (a: (Double, Int), b: (Double, Int)) => (a._1 + b._1, a._2 + b._2)
        )
      println("\nAverage transaction by category:")
      categoryTotals.mapValues { case (total, count) => total / count }
        .collect().foreach { case (cat, avg) =>
          println(f"  $cat%-15s -> $$$avg%.2f")
        }

      // Top 3 most expensive products
      val topProducts = transactions
        .map(parts => (parts(3).toDouble, parts(1)))
        .sortByKey(ascending = false)
        .take(3)
      println("\nTop 3 most expensive products:")
      topProducts.foreach { case (price, product) =>
        println(f"  $product%-15s -> $$$price%.2f")
      }

      // Spending per user per category (nested aggregation)
      val userCategorySpend = transactions
        .map(parts => (s"user_${parts(0)} | ${parts(2)}", parts(3).toDouble))
        .reduceByKey(_ + _)
      println("\nSpending per user per category:")
      userCategorySpend.sortByKey().collect().foreach { case (key, total) =>
        println(f"  $key%-30s -> $$$total%.2f")
      }
    } finally {
      sc.stop()
    }
  }
}
