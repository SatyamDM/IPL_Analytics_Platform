ThisBuild / scalaVersion := "2.13.10"
libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "42.6.0",
  "redis.clients" % "jedis" % "4.4.3"
)
