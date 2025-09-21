ThisBuild / scalaVersion := "2.13.10"
ThisBuild / version := "1.0.0"

libraryDependencies ++= Seq(
  // Database & Caching
  "org.postgresql" % "postgresql" % "42.6.0",
  "redis.clients" % "jedis" % "4.4.3",
  "org.json" % "json" % "20231013",

  // Apache Spark
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-mllib" % "3.5.0",

  // Kafka for streaming (future use)
  "org.apache.spark" %% "spark-streaming" % "3.5.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0"
)

// Resolve version conflicts
dependencyOverrides ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % "2.1.0"
)