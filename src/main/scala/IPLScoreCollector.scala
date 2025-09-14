import java.sql.{Connection, DriverManager}
import java.util.Timer
import java.util.TimerTask
import redis.clients.jedis.Jedis

object IPLScoreCollector {

  def main(args: Array[String]): Unit = {
    println("🏏 IPL Score Collector Starting...")

    // Test database connections
    testConnections()

    // Start collecting scores every 30 seconds
    startCollection()

    // Keep the program running
    println("✅ IPL Score Collector is running...")
    println("📊 Check Docker Desktop to see your databases")
    println("🔄 Collecting IPL data every 30 seconds")

    // Wait for user input to stop
    println("\nPress Enter to stop...")
    scala.io.StdIn.readLine()

    println("🛑 IPL Score Collector stopped")
  }

  def testConnections(): Unit = {
    println("\n🔍 Testing Database Connections...")

    // Test PostgreSQL
    try {
      val conn = DriverManager.getConnection(
        "jdbc:postgresql://localhost:5432/ipl", "user", "pass"
      )
      println("✅ PostgreSQL connected successfully")


      // Create a simple table for IPL matches
      val stmt = conn.createStatement()
      // Drop existing table and recreate with UNIQUE constraint
      stmt.execute("DROP TABLE IF EXISTS live_matches")
      stmt.execute("""
  CREATE TABLE IF NOT EXISTS live_matches (
    id SERIAL PRIMARY KEY,
    match_id VARCHAR(50) UNIQUE,
    team1 VARCHAR(100),
    team2 VARCHAR(100),
    score VARCHAR(200),
    status VARCHAR(50),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )
""")
      println("✅ Database table created/verified")
      conn.close()

    } catch {
      case e: Exception => println(s"❌ PostgreSQL connection failed: ${e.getMessage}")
    }

    // Test Redis
    try {
      val jedis = new Jedis("localhost", 6379)
      jedis.set("test", "IPL Analytics Connected!")
      val result = jedis.get("test")
      println(s"✅ Redis connected: $result")
      jedis.close()

    } catch {
      case e: Exception => println(s"❌ Redis connection failed: ${e.getMessage}")
    }
  }

  def startCollection(): Unit = {
    val timer = new Timer()

    timer.schedule(new TimerTask {
      def run(): Unit = {
        collectIPLData()
      }
    }, 0, 30000) // Run immediately, then every 30 seconds
  }

  def collectIPLData(): Unit = {
    println(s"\n⏰ ${java.time.LocalDateTime.now()}")
    println("🏏 Collecting IPL data...")

    // Simulate getting live match data (replace with real API later)
    val mockMatches = List(
      ("ipl_match_001", "Mumbai Indians", "Chennai Super Kings", "MI: 142/3 (15.2 ov)", "live"),
      ("ipl_match_002", "Royal Challengers Bangalore", "Delhi Capitals", "RCB: 89/2 (12.4 ov)", "live")
    )

    // Store in PostgreSQL
    try {
      val conn = DriverManager.getConnection(
        "jdbc:postgresql://localhost:5432/ipl", "user", "pass"
      )

      val stmt = conn.prepareStatement(
        "INSERT INTO live_matches (match_id, team1, team2, score, status) VALUES (?, ?, ?, ?, ?) " +
          "ON CONFLICT (match_id) DO UPDATE SET score = ?, status = ?, updated_at = CURRENT_TIMESTAMP"
      )

      mockMatches.foreach { case (matchId, team1, team2, score, status) =>
        stmt.setString(1, matchId)
        stmt.setString(2, team1)
        stmt.setString(3, team2)
        stmt.setString(4, score)
        stmt.setString(5, status)
        stmt.setString(6, score) // for UPDATE
        stmt.setString(7, status) // for UPDATE
        stmt.addBatch()

        println(s"📊 ${team1} vs ${team2}: ${score}")
      }

      stmt.executeBatch()
      stmt.close()
      conn.close()
      println("✅ Data stored in PostgreSQL")

    } catch {
      case e: Exception => println(s"❌ Database error: ${e.getMessage}")
    }

    // Cache latest scores in Redis
    try {
      val jedis = new Jedis("localhost", 6379)

      mockMatches.foreach { case (matchId, team1, team2, score, status) =>
        val matchData = s"$team1 vs $team2: $score"
        jedis.setex(s"live:$matchId", 300, matchData) // Expire in 5 minutes
      }

      jedis.close()
      println("✅ Latest scores cached in Redis")

    } catch {
      case e: Exception => println(s"❌ Redis error: ${e.getMessage}")
    }

    println("🎯 Collection cycle completed!")
  }
}