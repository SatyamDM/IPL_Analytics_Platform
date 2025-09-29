// File: IPLAnalytics.scala
// Production-ready IPL Analytics for Docker deployment
// Optimized for memory efficiency - No debugging code

package com.ipl.jobs

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util.Properties
import org.slf4j.LoggerFactory

/**
 * Production IPL Analytics Pipeline
 * Designed to run in Docker containers with external services
 */

object IPLSchema {
  val schema = StructType(Seq(
    StructField("info", StructType(Seq(
      StructField("balls_per_over", LongType, true),
      StructField("city", StringType, true),
      StructField("dates", ArrayType(StringType), true),
      StructField("gender", StringType, true),
      StructField("match_type", StringType, true),
      StructField("outcome", StructType(Seq(
        StructField("winner", StringType, true),
        StructField("by", StructType(Seq(
          StructField("runs", LongType, true),
          StructField("wickets", LongType, true)
        )), true)
      )), true),
      StructField("overs", LongType, true),
      StructField("player_of_match", ArrayType(StringType), true),
      StructField("season", StringType, true),
      StructField("team_type", StringType, true),
      StructField("teams", ArrayType(StringType), true),
      StructField("toss", StructType(Seq(
        StructField("decision", StringType, true),
        StructField("winner", StringType, true)
      )), true),
      StructField("venue", StringType, true),
      StructField("registry", StructType(Seq(
        StructField("people", MapType(StringType, StringType), true)
      )), true)
    )), true),
    StructField("innings", ArrayType(StructType(Seq(
      StructField("team", StringType, true),
      StructField("overs", ArrayType(StructType(Seq(
        StructField("over", LongType, true),
        StructField("deliveries", ArrayType(StructType(Seq(
          StructField("batter", StringType, true),
          StructField("bowler", StringType, true),
          StructField("non_striker", StringType, true),
          StructField("runs", StructType(Seq(
            StructField("batter", LongType, true),
            StructField("extras", LongType, true),
            StructField("total", LongType, true)
          )), true),
          StructField("wickets", ArrayType(StructType(Seq(
            StructField("player_out", StringType, true),
            StructField("kind", StringType, true)
          ))), true)
        ))), true)
      ))), true)
    ))), true),
    StructField("meta", StructType(Seq(
      StructField("created", StringType, true),
      StructField("data_version", StringType, true),
      StructField("revision", LongType, true)
    )), true)
  ))
}

object IPLAnalytics {

  private val logger = LoggerFactory.getLogger(getClass)

  // Production Configuration - Environment-based
  private val SPARK_MASTER = sys.env.getOrElse("SPARK_MASTER", "spark://spark-master:7077")
  private val POSTGRES_HOST = sys.env.getOrElse("POSTGRES_HOST", "ipl-postgres")
  private val POSTGRES_PORT = sys.env.getOrElse("POSTGRES_PORT", "5432")
  private val POSTGRES_DB = sys.env.getOrElse("POSTGRES_DB", "ipl_analytics")
  private val POSTGRES_USER = sys.env.getOrElse("POSTGRES_USER", "ipl_user")
  private val POSTGRES_PASSWORD = sys.env.getOrElse("POSTGRES_PASSWORD", "ipl_password")
  private val REDIS_HOST = sys.env.getOrElse("REDIS_HOST", "ipl-redis")
  private val REDIS_PORT = sys.env.getOrElse("REDIS_PORT", "6379")

  // Data paths - configurable via environment
  private val INPUT_DATA_PATH = sys.env.getOrElse("IPL_DATA_PATH", "/opt/bitnami/spark/data/ipl_json")
  private val OUTPUT_PATH = sys.env.getOrElse("IPL_OUTPUT_PATH", "/opt/bitnami/spark/output")

  def main(args: Array[String]): Unit = {
    logger.info("🚀 Starting Production IPL Analytics Pipeline...")

    val analytics = new IPLAnalytics()

    try {
      analytics.runProductionPipeline()
    } catch {
      case e: Exception =>
        logger.error("❌ Production pipeline failed", e)
        System.exit(1)
    }
  }
}

class IPLAnalytics {

  private val logger = LoggerFactory.getLogger(getClass)

  // Production Spark Session - Optimized for memory efficiency
  private val spark: SparkSession = SparkSession.builder()
    .appName("IPL-Analytics")
    .master(IPLAnalytics.SPARK_MASTER)
    .config("spark.executor.memory", "3g")
    .config("spark.driver.memory", "3g")
    .config("spark.executor.cores", "2")
    .config("spark.executor.instances", "2")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.memory.fraction", "0.8")
    .config("spark.memory.storageFraction", "0.3")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  logger.info("✅ Spark session initialized")
  logger.info(s"🔗 Master: ${spark.sparkContext.master}")
  logger.info(s"📊 Version: ${spark.version}")

  import spark.implicits._

  // PostgreSQL connection properties
  private val postgresProps = new Properties()
  postgresProps.setProperty("driver", "org.postgresql.Driver")
  postgresProps.setProperty("user", IPLAnalytics.POSTGRES_USER)
  postgresProps.setProperty("password", IPLAnalytics.POSTGRES_PASSWORD)

  private val postgresUrl = s"jdbc:postgresql://${IPLAnalytics.POSTGRES_HOST}:${IPLAnalytics.POSTGRES_PORT}/${IPLAnalytics.POSTGRES_DB}"

  /**
   * Load IPL data from container filesystem
   */
  def loadIPLData(): DataFrame = {
    logger.info(s"📂 Loading data from: ${IPLAnalytics.INPUT_DATA_PATH}")

    try {
      import org.apache.hadoop.fs.{FileSystem, Path}
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val basePath = new Path(IPLAnalytics.INPUT_DATA_PATH)

      if (!fs.exists(basePath)) {
        throw new RuntimeException(s"Path not found: ${IPLAnalytics.INPUT_DATA_PATH}")
      }

      val jsonFiles = fs.listStatus(basePath).filter(_.getPath.getName.endsWith(".json"))

      if (jsonFiles.isEmpty) {
        throw new RuntimeException(s"No JSON files in ${IPLAnalytics.INPUT_DATA_PATH}")
      }

      logger.info(s"✅ Found ${jsonFiles.length} JSON files")

      val jsonPaths = jsonFiles.map(_.getPath.toString)

      val allDf = spark.read
        .option("multiline", "true")
        .option("mode", "PERMISSIVE")
        .json(jsonPaths: _*)

      logger.info("✅ Data loaded successfully")
      allDf

    } catch {
      case e: Exception =>
        logger.error(s"❌ Failed to load data: ${e.getMessage}")
        throw e
    }
  }

  /**
   * Clean and transform IPL data
   */
  def transformIPLData(rawDf: DataFrame): (DataFrame, DataFrame) = {
    logger.info("🧹 Transforming data...")

    val matchesDf = rawDf
      .select(
        coalesce(col("info.dates").getItem(0), lit("1900-01-01")).cast("date").as("match_date"),
        coalesce(col("info.venue"), lit("Unknown")).as("venue"),
        col("info.teams").as("teams"),
        coalesce(col("info.season"), lit("Unknown")).as("season"),
        coalesce(col("info.match_type"), lit("T20")).as("match_type"),
        col("info.outcome.winner").as("winner"),
        col("info.outcome.by.runs").as("win_by_runs"),
        col("info.outcome.by.wickets").as("win_by_wickets"),
        col("info.toss.winner").as("toss_winner"),
        col("info.toss.decision").as("toss_decision"),
        col("info.player_of_match").as("player_of_match"),
        col("info.registry.people").as("players_registry"),
        col("innings").as("innings_data"),
        current_timestamp().as("processed_at")
      )
      .withColumn("match_id", monotonically_increasing_id())

    logger.info("✅ Matches transformed")

    val ballsByBallDf = rawDf
      .select(
        monotonically_increasing_id().as("match_id"),
        coalesce(col("info.season"), lit("Unknown")).as("season"),
        col("info.teams").as("teams"),
        col("info.venue").as("venue"),
        col("info.dates").getItem(0).cast("date").as("match_date"),
        explode(col("innings")).as("innings")
      )
      .filter(col("innings.overs").isNotNull && size(col("innings.overs")) > 0)
      .select(
        col("match_id"), col("season"), col("teams"), col("venue"), col("match_date"),
        col("innings.team").as("batting_team"),
        explode(col("innings.overs")).as("over_data")
      )
      .filter(col("over_data.deliveries").isNotNull && size(col("over_data.deliveries")) > 0)
      .select(
        col("match_id"), col("season"), col("teams"), col("venue"), col("match_date"),
        col("batting_team"),
        col("over_data.over").as("over_number"),
        explode(col("over_data.deliveries")).as("delivery")
      )
      .select(
        col("match_id"), col("season"), col("teams"), col("venue"), col("match_date"),
        col("batting_team"), col("over_number"),
        coalesce(col("delivery.batter"), lit("Unknown")).as("batter"),
        coalesce(col("delivery.bowler"), lit("Unknown")).as("bowler"),
        coalesce(col("delivery.non_striker"), lit("Unknown")).as("non_striker"),
        coalesce(col("delivery.runs.batter"), lit(0)).as("runs_batter"),
        coalesce(col("delivery.runs.extras"), lit(0)).as("runs_extras"),
        coalesce(col("delivery.runs.total"), lit(0)).as("runs_total"),
        when(col("delivery.wickets").isNotNull && size(col("delivery.wickets")) > 0, 1)
          .otherwise(0).as("is_wicket"),
        when(col("delivery.wickets").isNotNull && size(col("delivery.wickets")) > 0,
          concat_ws(", ",
            transform(col("delivery.wickets"),
              w => concat(w.getField("player_out"), lit(" - "), w.getField("kind"))
            )
          )
        ).otherwise(lit(null)).as("wicket_details"),
        current_timestamp().as("processed_at")
      )
      .filter(col("season") =!= "Unknown" &&
        col("batter") =!= "Unknown" &&
        col("bowler") =!= "Unknown")
      .withColumn("ball_id", monotonically_increasing_id())

    logger.info("✅ Ball-by-ball transformed")

    (matchesDf, ballsByBallDf)
  }

  /**
   * Calculate statistics
   */
  def calculateProductionStats(matchesDf: DataFrame, ballsDf: DataFrame): Map[String, DataFrame] = {
    logger.info("📊 Calculating stats...")

    val teamStats = matchesDf
      .withColumn("team", explode(col("teams")))
      .groupBy(col("season"), col("team"))
      .agg(
        countDistinct("match_id").as("matches_played"),
        sum(when(col("winner") === col("team"), 1).otherwise(0)).as("matches_won"),
        sum(when(col("toss_winner") === col("team"), 1).otherwise(0)).as("tosses_won")
      )
      .withColumn("win_percentage",
        round((col("matches_won").cast("double") / col("matches_played")) * 100, 2))
      .withColumn("toss_win_percentage",
        round((col("tosses_won").cast("double") / col("matches_played")) * 100, 2))
      .orderBy(col("season"), col("win_percentage").desc)

    val playerBattingStats = ballsDf
      .groupBy(col("season"), col("batter"))
      .agg(
        sum("runs_batter").as("total_runs"),
        count("*").as("balls_faced"),
        countDistinct("match_id").as("matches_played"),
        max("runs_batter").as("highest_score"),
        sum(when(col("runs_batter") === 4, 1).otherwise(0)).as("fours"),
        sum(when(col("runs_batter") === 6, 1).otherwise(0)).as("sixes"),
        sum(when(col("is_wicket") === 1, 1).otherwise(0)).as("times_out")
      )
      .withColumn("strike_rate",
        round((col("total_runs").cast("double") / col("balls_faced")) * 100, 2))
      .withColumn("average",
        when(col("times_out") > 0,
          round(col("total_runs").cast("double") / col("times_out"), 2))
          .otherwise(lit(null)))
      .withColumn("boundary_percentage",
        round(((col("fours") + col("sixes")).cast("double") / col("balls_faced")) * 100, 2))
      .filter(col("balls_faced") >= 50)
      .orderBy(col("season"), col("total_runs").desc)

    val playerBowlingStats = ballsDf
      .groupBy(col("season"), col("bowler"))
      .agg(
        count("*").as("balls_bowled"),
        sum("runs_total").as("runs_conceded"),
        countDistinct("match_id").as("matches_played"),
        sum("is_wicket").as("wickets_taken"),
        sum(when(col("runs_total") === 0, 1).otherwise(0)).as("dot_balls")
      )
      .withColumn("overs_bowled",
        round(col("balls_bowled").cast("double") / 6, 1))
      .withColumn("economy_rate",
        round((col("runs_conceded").cast("double") / col("overs_bowled")), 2))
      .withColumn("bowling_average",
        when(col("wickets_taken") > 0,
          round(col("runs_conceded").cast("double") / col("wickets_taken"), 2))
          .otherwise(lit(null)))
      .withColumn("dot_ball_percentage",
        round((col("dot_balls").cast("double") / col("balls_bowled")) * 100, 2))
      .filter(col("balls_bowled") >= 60)
      .orderBy(col("season"), col("economy_rate"))

    val seasonTrends = ballsDf
      .groupBy(col("season"))
      .agg(
        avg("runs_total").as("avg_runs_per_ball"),
        sum("runs_total").as("total_runs"),
        count("*").as("total_balls"),
        countDistinct("match_id").as("total_matches"),
        avg("is_wicket").as("wicket_rate")
      )
      .withColumn("avg_runs_per_match",
        round(col("total_runs").cast("double") / col("total_matches"), 2))
      .withColumn("avg_runs_per_over",
        round(col("avg_runs_per_ball") * 6, 2))
      .orderBy(col("season"))

    val venueStats = ballsDf
      .groupBy(col("venue"))
      .agg(
        countDistinct("match_id").as("matches_played"),
        avg("runs_total").as("avg_runs_per_ball"),
        count("*").as("total_balls")
      )
      .withColumn("avg_runs_per_over",
        round(col("avg_runs_per_ball") * 6, 2))
      .filter(col("matches_played") >= 10)
      .orderBy(col("avg_runs_per_over").desc)

    logger.info("✅ Stats calculated")

    Map(
      "team_stats" -> teamStats,
      "player_batting_stats" -> playerBattingStats,
      "player_bowling_stats" -> playerBowlingStats,
      "season_trends" -> seasonTrends,
      "venue_stats" -> venueStats
    )
  }

  /**
   * Save to PostgreSQL
   */
  def saveToPostgreSQL(dataFrames: Map[String, DataFrame]): Unit = {
    logger.info("💾 Saving to PostgreSQL...")

    try {
      dataFrames.foreach { case (tableName, df) =>
        logger.info(s"📊 Saving $tableName")

        df.write
          .mode("overwrite")
          .jdbc(postgresUrl, s"ipl_$tableName", postgresProps)

        logger.info(s"✅ Saved $tableName")
      }

    } catch {
      case e: Exception =>
        logger.error(s"❌ PostgreSQL failed: ${e.getMessage}")

        logger.info("💾 Fallback: Saving to Parquet...")
        dataFrames.foreach { case (tableName, df) =>
          val outputPath = s"${IPLAnalytics.OUTPUT_PATH}/$tableName"
          df.write.mode("overwrite").parquet(outputPath)
          logger.info(s"✅ Saved $tableName to Parquet")
        }
    }
  }

  /**
   * Display insights
   */
  def displayProductionInsights(stats: Map[String, DataFrame]): Unit = {
    logger.info("📊 Displaying insights...")

    println("\n" + "="*60)
    println("🏏 IPL ANALYTICS PRODUCTION SUMMARY")
    println("="*60)

    println("\n👑 TOP 10 RUN SCORERS:")
    stats("player_batting_stats")
      .orderBy(col("total_runs").desc)
      .select("batter", "total_runs", "strike_rate", "average")
      .show(10, false)

    println("\n⚾ TOP 10 BOWLERS:")
    stats("player_bowling_stats")
      .orderBy(col("economy_rate"))
      .select("bowler", "wickets_taken", "economy_rate", "bowling_average")
      .show(10, false)

    println("\n🏆 TEAM PERFORMANCE:")
    stats("team_stats")
      .orderBy(col("win_percentage").desc)
      .show(10, false)

    println("\n📈 SEASON TRENDS:")
    stats("season_trends").show(false)

    println("\n🏟️ TOP VENUES:")
    stats("venue_stats").show(10, false)

    println("\n✅ Pipeline completed successfully!")
    println("="*60)
  }

  /**
   * Run pipeline
   */
  def runProductionPipeline(): Unit = {
    logger.info("🚀 Starting pipeline...")

    try {
      // Step 1: Load data
      val rawDf = loadIPLData()

      // Step 2: Transform data
      val (matchesDf, ballsDf) = transformIPLData(rawDf)

      // Step 3: Calculate statistics (no caching to save memory)
      val stats = calculateProductionStats(matchesDf, ballsDf)

      // Step 4: Prepare data for database
      val matchesForDB = matchesDf.drop("innings_data", "players_registry")
      val allDataFrames = stats ++ Map(
        "matches" -> matchesForDB,
        "balls_by_ball" -> ballsDf
      )

      // Step 5: Save to database
      saveToPostgreSQL(allDataFrames)

      // Step 6: Display insights
      displayProductionInsights(stats)

      logger.info("🎉 Pipeline completed successfully!")

    } catch {
      case e: Exception =>
        logger.error(s"❌ Pipeline failed: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      logger.info("🧹 Cleanup...")
      spark.stop()
    }
  }
}