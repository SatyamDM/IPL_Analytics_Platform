package com.ipl.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object SparkSessionBuilder {

  def createSparkSession(appName: String): SparkSession = {
    println(s"🔧 Creating Spark session for: $appName")

    // Try cluster mode first, fallback to local
    createClusterSparkSession(appName).getOrElse {
      println("⚠️ Cluster mode failed, using local mode")
      createLocalSparkSession(appName)
    }
  }

  def createClusterSparkSession(appName: String): Option[SparkSession] = {
    try {
      println("🔗 Attempting cluster connection...")

      val conf = new SparkConf()
        .setAppName(appName)
        .setMaster("spark://localhost:7077")
        .set("spark.sql.adaptive.enabled", "true")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.network.timeout", "10s")  // Quick timeout
        .set("spark.rpc.askTimeout", "10s")

      val spark = SparkSession.builder()
        .config(conf)
        .getOrCreate()

      // Test the connection immediately
      spark.sparkContext.setLogLevel("WARN")

      // Simple test to verify cluster is working
      val testRDD = spark.sparkContext.parallelize(Seq(1, 2, 3))
      testRDD.count() // This will fail if cluster isn't working

      println("✅ Cluster connection successful!")
      println(s"📊 Spark UI available at: http://localhost:4040")
      Some(spark)

    } catch {
      case e: Exception =>
        println(s"❌ Cluster connection failed: ${e.getMessage}")
        None
    }
  }

  def createLocalSparkSession(appName: String): SparkSession = {
    println("🏠 Creating local Spark session...")

    val spark = SparkSession.builder()
      .appName(s"$appName-Local")
      .master("local[*]")  // Use all available cores
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("✅ Local Spark session created!")
    println(s"📊 Spark UI available at: http://localhost:4040")
    spark
  }

  def stopSparkSession(spark: SparkSession): Unit = {
    println("🛑 Stopping Spark session...")
    try {
      spark.stop()
      println("✅ Spark session stopped")
    } catch {
      case e: Exception =>
        println(s"⚠️ Warning during shutdown: ${e.getMessage}")
    }
  }
}