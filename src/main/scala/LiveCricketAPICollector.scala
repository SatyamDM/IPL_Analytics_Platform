import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.{Timer, TimerTask}
import java.net.{HttpURLConnection, URL}
import java.io.BufferedReader
import java.io.InputStreamReader
import redis.clients.jedis.Jedis
import scala.util.{Try, Success, Failure}
import org.json.{JSONObject, JSONArray}

object LiveCricketAPICollector {

  // Replace with your actual API key
  val API_KEY = "ee5156fd-b4f9-4e68-b81c-c243a4bf970f"
  val CRICAPI_URL = s"https://api.cricapi.com/v1/currentMatches?apikey=$API_KEY&offset=0"

  def main(args: Array[String]): Unit = {
    println("🏏 Starting Live Cricket Data Collector...")

    // Test connections first
    testConnections()

    // Create database tables
    setupDatabase()

    // Start data collection
    startLiveCollection()
  }

  def testConnections(): Unit = {
    println("\n🔧 Testing connections...")

    // Test database
    Try {
      val conn = DriverManager.getConnection(
        "jdbc:postgresql://localhost:5432/ipl_analytics", "ipl_user", "ipl_password"
      )
      println("✅ PostgreSQL connected")
      conn.close()
    } match {
      case Failure(e) => println(s"❌ Database connection failed: ${e.getMessage}")
      case Success(_) => ()
    }

    // Test Redis
    Try {
      val jedis = new Jedis("localhost", 6379)
      jedis.ping()
      println("✅ Redis connected")
      jedis.close()
    } match {
      case Failure(e) => println(s"❌ Redis connection failed: ${e.getMessage}")
      case Success(_) => ()
    }

    // Test Cricket API
    testCricketAPI()
  }

  def testCricketAPI(): Unit = {
    println("🌐 Testing Cricket API...")

    Try {
      val response = makeAPICall(CRICAPI_URL)
      val json = new JSONObject(response)

      if (json.has("data")) {
        val matches = json.getJSONArray("data")
        println(s"✅ Cricket API working - Found ${matches.length()} matches")

        // Show sample data
        if (matches.length() > 0) {
          val firstMatch = matches.getJSONObject(0)
          val teams = firstMatch.getJSONArray("teamInfo")
          val team1 = teams.getJSONObject(0).getString("name")
          val team2 = teams.getJSONObject(1).getString("name")
          println(s"📊 Sample match: $team1 vs $team2")
        }
      } else {
        println("⚠️ API response format unexpected")
        println(s"Response: ${response.take(200)}...")
      }
    } match {
      case Failure(e) =>
        println(s"❌ Cricket API failed: ${e.getMessage}")
        println("💡 Using fallback mock data for now...")
      case Success(_) => ()
    }
  }

  def makeAPICall(urlString: String): String = {
    val url = new URL(urlString)
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]

    connection.setRequestMethod("GET")
    connection.setConnectTimeout(10000) // 10 seconds
    connection.setReadTimeout(10000)

    val responseCode = connection.getResponseCode

    if (responseCode == HttpURLConnection.HTTP_OK) {
      val reader = new BufferedReader(new InputStreamReader(connection.getInputStream))
      val response = new StringBuilder
      var line: String = null

      while ({line = reader.readLine(); line != null}) {
        response.append(line)
      }
      reader.close()
      response.toString
    } else {
      throw new RuntimeException(s"HTTP Error: $responseCode")
    }
  }

  def setupDatabase(): Unit = {
    Try {
      val conn = DriverManager.getConnection(
        "jdbc:postgresql://localhost:5432/ipl_analytics", "ipl_user", "ipl_password"
      )

      val stmt = conn.createStatement()

      // Enhanced table for live cricket data
      stmt.execute("""
        CREATE TABLE IF NOT EXISTS live_matches (
          id SERIAL PRIMARY KEY,
          match_id VARCHAR(100) UNIQUE,
          team1 VARCHAR(100),
          team2 VARCHAR(100),
          team1_score VARCHAR(200),
          team2_score VARCHAR(200),
          status VARCHAR(200),
          is_ongoing BOOLEAN DEFAULT FALSE,
          is_completed BOOLEAN DEFAULT FALSE,
          venue VARCHAR(200),
          date_time VARCHAR(100),
          toss_winner VARCHAR(100),
          current_over DECIMAL(4,1),
          balls_remaining INTEGER,
          target INTEGER,
          required_rate DECIMAL(4,2),
          winner VARCHAR(100),
          result_margin VARCHAR(100),
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      """)

      // Ball by ball data table
      stmt.execute("""
        CREATE TABLE IF NOT EXISTS live_balls (
          id SERIAL PRIMARY KEY,
          match_id VARCHAR(100),
          over_number INTEGER,
          ball_number INTEGER,
          batsman VARCHAR(100),
          bowler VARCHAR(100),
          runs INTEGER,
          is_wicket BOOLEAN DEFAULT FALSE,
          commentary TEXT,
          timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          FOREIGN KEY (match_id) REFERENCES live_matches(match_id)
        )
      """)

      stmt.close()
      conn.close()
      println("✅ Database tables created/verified")

    } match {
      case Failure(e) => println(s"❌ Database setup failed: ${e.getMessage}")
      case Success(_) => ()
    }
  }

  def startLiveCollection(): Unit = {
    val timer = new Timer()

    timer.schedule(new TimerTask {
      def run(): Unit = {
        collectLiveCricketData()
      }
    }, 0, 60000) // Every 60 seconds
  }

  def collectLiveCricketData(): Unit = {
    println(s"\n⏰ ${java.time.LocalDateTime.now()}")
    println("🏏 Fetching live cricket data...")

    Try {
      val response = makeAPICall(CRICAPI_URL)
      val json = new JSONObject(response)

      if (json.has("data")) {
        val matches = json.getJSONArray("data")

        if (matches.length() > 0) {
          processLiveMatches(matches)
        } else {
          println("📭 No live matches found, using sample data...")
          processFallbackData()
        }
      } else {
        println("⚠️ Unexpected API response format")
        processFallbackData()
      }

    } match {
      case Failure(e) =>
        println(s"❌ API call failed: ${e.getMessage}")
        println("🔄 Using fallback data...")
        processFallbackData()
      case Success(_) => ()
    }
  }

  def processLiveMatches(matches: JSONArray): Unit = {
    println(s"📊 Processing ${matches.length()} live matches...")

    for (i <- 0 until matches.length()) {
      val matchData = matches.getJSONObject(i)

      Try {
        val matchId = matchData.getString("id")
        val teams = matchData.getJSONArray("teamInfo")
        val team1 = teams.getJSONObject(0).getString("name")
        val team2 = teams.getJSONObject(1).getString("name")
        val status = matchData.getString("status")
        val venue = if (matchData.has("venue")) matchData.getString("venue") else "Unknown"

        // Determine match state flags
        val (isOngoing, isCompleted) = determineMatchState(status)

        // Get scores and live stats if available
        val (team1Score, team2Score, currentOver, target, reqRate) = if (matchData.has("score")) {
          extractDetailedScores(matchData.getJSONArray("score"), status)
        } else {
          ("Not started", "Not started", 0.0, 0, 0.0)
        }

        // Get winner and result if completed
        val (winner, resultMargin) = if (isCompleted && matchData.has("status")) {
          extractResult(matchData, team1, team2)
        } else {
          ("", "")
        }

        // Display match info with enhanced details
        println(s"🏟️ $team1 vs $team2 at $venue")
        println(s"📈 Score: $team1Score | $team2Score")
        println(s"⚡ Status: $status ${if (isOngoing) "🔴 LIVE" else if (isCompleted) "✅ COMPLETED" else "⏳ UPCOMING"}")

        if (isOngoing && currentOver > 0) {
          println(s"🎯 Live Stats: Over ${currentOver} | Target: ${if (target > 0) target else "N/A"} | Req Rate: ${if (reqRate > 0) f"$reqRate%.2f" else "N/A"}")
        }

        if (isCompleted && winner.nonEmpty) {
          println(s"🏆 Winner: $winner ($resultMargin)")
        }

        // Store in database with enhanced data
        storeEnhancedMatchData(matchId, team1, team2, team1Score, team2Score, status,
          isOngoing, isCompleted, venue, currentOver, target, reqRate, winner, resultMargin)

        // Cache in Redis
        cacheMatchData(matchId, team1, team2, team1Score, team2Score, status)

      } match {
        case Failure(e) => println(s"❌ Error processing match: ${e.getMessage}")
        case Success(_) => ()
      }
    }

    println("✅ Live data processing completed!")
  }

  def determineMatchState(status: String): (Boolean, Boolean) = {
    val statusLower = status.toLowerCase
    val isOngoing = statusLower.contains("live") || statusLower.contains("in progress") ||
      statusLower.contains("innings break") || statusLower.contains("batting") ||
      statusLower.contains("bowling")
    val isCompleted = statusLower.contains("won") || statusLower.contains("complete") ||
      statusLower.contains("finished") || statusLower.contains("result")
    (isOngoing, isCompleted)
  }

  def extractDetailedScores(scoresArray: JSONArray, status: String): (String, String, Double, Int, Double) = {
    if (scoresArray.length() >= 1) {
      val team1Score = scoresArray.getJSONObject(0)
      var team2Score: JSONObject = null

      // Get basic scores
      val score1 = if (team1Score.has("r") && team1Score.has("w") && team1Score.has("o")) {
        s"${team1Score.getInt("r")}/${team1Score.getInt("w")} (${team1Score.getDouble("o")} ov)"
      } else {
        "0/0 (0.0)"
      }

      val score2 = if (scoresArray.length() >= 2) {
        team2Score = scoresArray.getJSONObject(1)
        if (team2Score.has("r") && team2Score.has("w") && team2Score.has("o")) {
          s"${team2Score.getInt("r")}/${team2Score.getInt("w")} (${team2Score.getDouble("o")} ov)"
        } else {
          "0/0 (0.0)"
        }
      } else {
        "Yet to bat"
      }

      // Extract live stats for ongoing matches
      val currentOver = if (team2Score != null && team2Score.has("o")) {
        team2Score.getDouble("o")
      } else if (team1Score.has("o")) {
        team1Score.getDouble("o")
      } else {
        0.0
      }

      // Calculate target and required rate
      val (target, reqRate) = if (scoresArray.length() >= 2 && team1Score.has("r") && team2Score != null) {
        val team1Runs = team1Score.getInt("r")
        val target = team1Runs + 1

        val team2Runs = if (team2Score.has("r")) team2Score.getInt("r") else 0
        val team2Overs = if (team2Score.has("o")) team2Score.getDouble("o") else 0.0
        val remainingOvers = 20.0 - team2Overs

        val reqRate = if (remainingOvers > 0) {
          (target - team2Runs).toDouble / remainingOvers
        } else {
          0.0
        }

        (target, reqRate)
      } else {
        (0, 0.0)
      }

      (score1, score2, currentOver, target, reqRate)
    } else {
      ("Not started", "Not started", 0.0, 0, 0.0)
    }
  }

  def extractResult(matchData: JSONObject, team1: String, team2: String): (String, String) = {
    val status = matchData.getString("status")

    // Try to parse winner from status text
    if (status.toLowerCase.contains("won")) {
      if (status.toLowerCase.contains(team1.toLowerCase.split(" ").last)) {
        (team1, extractMargin(status))
      } else if (status.toLowerCase.contains(team2.toLowerCase.split(" ").last)) {
        (team2, extractMargin(status))
      } else {
        ("", status)
      }
    } else {
      ("", status)
    }
  }

  def extractMargin(status: String): String = {
    // Extract winning margin from status text
    val patterns = List(
      """(\d+)\s+runs?""".r,
      """(\d+)\s+wickets?""".r,
      """(\d+)\s+balls?""".r
    )

    patterns.foreach { pattern =>
      pattern.findFirstIn(status.toLowerCase) match {
        case Some(margin) => return margin
        case None =>
      }
    }

    // If no specific margin found, return the status
    status
  }

  def processFallbackData(): Unit = {
    // Enhanced mock data with different match states
    val mockMatches = List(
      ("live_001", "Mumbai Indians", "Chennai Super Kings", "MI: 142/3 (15.2)", "CSK: 98/2 (12.4)", "CSK needs 45 runs in 32 balls", true, false, "Wankhede Stadium", 12.4, 143, 8.44, "", ""),
      ("live_002", "Royal Challengers Bangalore", "Delhi Capitals", "RCB: 167/4 (20.0)", "DC: 123/5 (15.6)", "DC needs 45 runs in 26 balls", true, false, "M. Chinnaswamy Stadium", 15.6, 168, 10.38, "", ""),
      ("comp_001", "Kolkata Knight Riders", "Punjab Kings", "KKR: 189/6 (20.0)", "PBKS: 145/7 (20.0)", "KKR won by 44 runs", false, true, "Eden Gardens", 20.0, 190, 0.0, "Kolkata Knight Riders", "44 runs"),
      ("upcoming_001", "Rajasthan Royals", "Sunrisers Hyderabad", "Not started", "Not started", "Match starts at 7:30 PM", false, false, "Sawai Mansingh Stadium", 0.0, 0, 0.0, "", "")
    )

    mockMatches.foreach { case (matchId, team1, team2, score1, score2, status, isOngoing, isCompleted, venue, currentOver, target, reqRate, winner, margin) =>
      println(s"🏟️ $team1 vs $team2 at $venue")
      println(s"📈 Score: $score1 | $score2")
      println(s"⚡ Status: $status ${if (isOngoing) "🔴 LIVE" else if (isCompleted) "✅ COMPLETED" else "⏳ UPCOMING"}")

      if (isOngoing && currentOver > 0) {
        println(s"🎯 Live Stats: Over ${currentOver} | Target: ${if (target > 0) target else "N/A"} | Req Rate: ${if (reqRate > 0) f"$reqRate%.2f" else "N/A"}")
      }

      if (isCompleted && winner.nonEmpty) {
        println(s"🏆 Winner: $winner ($margin)")
      }

      storeEnhancedMatchData(matchId, team1, team2, score1, score2, status, isOngoing, isCompleted, venue, currentOver, target, reqRate, winner, margin)
      cacheMatchData(matchId, team1, team2, score1, score2, status)
    }

    println("✅ Fallback data processing completed!")
  }

  def storeEnhancedMatchData(matchId: String, team1: String, team2: String,
                             score1: String, score2: String, status: String,
                             isOngoing: Boolean, isCompleted: Boolean, venue: String,
                             currentOver: Double, target: Int, reqRate: Double,
                             winner: String, resultMargin: String): Unit = {
    Try {
      val conn = DriverManager.getConnection(
        "jdbc:postgresql://localhost:5432/ipl_analytics", "ipl_user", "ipl_password"
      )

      val stmt = conn.prepareStatement("""
        INSERT INTO live_matches (match_id, team1, team2, team1_score, team2_score, status,
                                 is_ongoing, is_completed, venue, current_over, target,
                                 required_rate, winner, result_margin)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (match_id) DO UPDATE SET
          team1_score = ?, team2_score = ?, status = ?, is_ongoing = ?, is_completed = ?,
          current_over = ?, target = ?, required_rate = ?, winner = ?, result_margin = ?,
          updated_at = CURRENT_TIMESTAMP
      """)

      // Insert values
      stmt.setString(1, matchId)
      stmt.setString(2, team1)
      stmt.setString(3, team2)
      stmt.setString(4, score1)
      stmt.setString(5, score2)
      stmt.setString(6, status)
      stmt.setBoolean(7, isOngoing)
      stmt.setBoolean(8, isCompleted)
      stmt.setString(9, venue)
      stmt.setDouble(10, currentOver)
      stmt.setInt(11, target)
      stmt.setDouble(12, reqRate)
      stmt.setString(13, winner)
      stmt.setString(14, resultMargin)

      // Update values (for conflict resolution)
      stmt.setString(15, score1)
      stmt.setString(16, score2)
      stmt.setString(17, status)
      stmt.setBoolean(18, isOngoing)
      stmt.setBoolean(19, isCompleted)
      stmt.setDouble(20, currentOver)
      stmt.setInt(21, target)
      stmt.setDouble(22, reqRate)
      stmt.setString(23, winner)
      stmt.setString(24, resultMargin)

      stmt.executeUpdate()
      stmt.close()
      conn.close()

    } match {
      case Failure(e) => println(s"❌ Database storage failed: ${e.getMessage}")
      case Success(_) => ()
    }
  }

  def cacheMatchData(matchId: String, team1: String, team2: String,
                     score1: String, score2: String, status: String): Unit = {
    Try {
      val jedis = new Jedis("localhost", 6379)

      val matchData = s"""{"teams":"$team1 vs $team2","score1":"$score1","score2":"$score2","status":"$status"}"""
      jedis.setex(s"live:$matchId", 600, matchData) // 10 minutes expiry

      jedis.close()

    } match {
      case Failure(e) => println(s"❌ Redis caching failed: ${e.getMessage}")
      case Success(_) => ()
    }
  }
}