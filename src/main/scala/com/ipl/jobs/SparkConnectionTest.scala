package com.ipl.jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import com.ipl.utils.SparkSessionBuilder

object SparkConnectionTest {

  def main(args: Array[String]): Unit = {
    println("🚀 Testing Spark Connection...")

    val spark = SparkSessionBuilder.createSparkSession("IPL-Connection-Test")

    try {
      runSparkTests(spark)
    } catch {
      case e: Exception =>
        println(s"❌ Test failed: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      SparkSessionBuilder.stopSparkSession(spark)
    }
  }

  def runSparkTests(spark: SparkSession): Unit = {
    // Import implicits for DataFrame operations
    import spark.implicits._

    // Test 1: Basic Spark functionality
    println("\n📊 Test 1: Basic Spark Operations")

    val testData = Seq(
      ("Mumbai Indians", 5, 2019),
      ("Chennai Super Kings", 4, 2021),
      ("Kolkata Knight Riders", 2, 2014)
    ).toDF("team", "titles", "last_won")

    println("Sample IPL data:")
    testData.show()

    // Test 2: Spark SQL
    println("\n📊 Test 2: Spark SQL")
    testData.createOrReplaceTempView("ipl_teams")
    val sqlResult = spark.sql("SELECT team, titles FROM ipl_teams WHERE titles > 2")
    sqlResult.show()

    // Test 3: Aggregations (SIMPLIFIED)
    println("\n📊 Test 3: Aggregations")
    val totalTitles = testData.select(sum("titles")).collect()(0)(0)
    println(s"Total IPL titles distributed: $totalTitles")

    // Test 4: Alternative aggregation using SQL
    println("\n📊 Test 4: SQL Aggregation")
    val sqlTotal = spark.sql("SELECT SUM(titles) as total FROM ipl_teams").collect()(0)(0)
    println(s"Total titles via SQL: $sqlTotal")

    // Test 5: Check Spark info
    println("\n🔧 Spark Cluster Info:")
    println(s"Spark Version: ${spark.version}")
    println(s"Master: ${spark.sparkContext.master}")
    println(s"App Name: ${spark.sparkContext.appName}")
    println(s"Default Parallelism: ${spark.sparkContext.defaultParallelism}")

    println("\n✅ All tests passed! Spark is working perfectly!")
  }
}