# 🏏 IPL Analytics - Big Data Pipeline

A production-ready big data analytics platform for Indian Premier League (IPL) cricket data using Apache Spark, Kafka, PostgreSQL, and Docker.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Project Structure](#project-structure)
- [Usage](#usage)
- [Configuration](#configuration)
- [API Documentation](#api-documentation)
- [Troubleshooting](#troubleshooting)
- [Performance Tuning](#performance-tuning)
- [Contributing](#contributing)
- [License](#license)

## 🎯 Overview

This project provides a comprehensive analytics platform for IPL cricket data, processing ball-by-ball match data from JSON files to generate insights including:

- **Player Statistics**: Batting averages, strike rates, bowling economy
- **Team Performance**: Win percentages, toss statistics
- **Season Trends**: Run rates, scoring patterns over time
- **Venue Analysis**: Ground-specific statistics

The platform is built with scalability in mind, using distributed computing frameworks and containerization for easy deployment.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Data Sources                         │
│              (IPL JSON Match Data)                      │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│              Apache Spark Cluster                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │ Spark Master │  │ Spark Worker │  │ Spark Worker │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
│         (Data Processing & Analytics)                   │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│              Storage Layer                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │  PostgreSQL  │  │    Redis     │  │   Parquet    │  │
│  │  (Metadata)  │  │   (Cache)    │  │   (Backup)   │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────┘
```

### Technology Stack

- **Compute**: Apache Spark 3.5.0
- **Database**: PostgreSQL 15
- **Cache**: Redis 7
- **Streaming**: Apache Kafka 7.4.0
- **Orchestration**: Docker Compose
- **Language**: Scala 2.12
- **Build Tool**: SBT

## ✨ Features

### Data Processing
- ✅ **Distributed Processing**: Scalable Spark cluster with multiple workers
- ✅ **Memory Optimized**: Efficient memory management for large datasets
- ✅ **Fault Tolerant**: Automatic retry and fallback mechanisms
- ✅ **Schema Validation**: Predefined schema for data quality

### Analytics
- 📊 **Player Batting Stats**: Runs, strike rate, average, boundaries
- 🎯 **Player Bowling Stats**: Wickets, economy rate, dot ball %
- 🏆 **Team Performance**: Win %, toss impact analysis
- 📈 **Season Trends**: Historical analysis across seasons
- 🏟️ **Venue Statistics**: Ground-specific scoring patterns

### Infrastructure
- 🐳 **Containerized**: Easy deployment with Docker
- 🔄 **Real-time Streaming**: Kafka integration for live data
- 💾 **Dual Storage**: PostgreSQL for queries + Parquet for backups
- 🎛️ **Monitoring**: Spark UI, Kafka UI for observability
- 📓 **Jupyter Integration**: Interactive data exploration

## 📦 Prerequisites

- **Docker**: 20.10+ and Docker Compose 2.0+
- **Memory**: Minimum 16GB RAM recommended
- **Storage**: 10GB free disk space
- **CPU**: 4+ cores recommended
- **OS**: Linux, macOS, or Windows with WSL2

### For Development
- **Scala**: 2.12.x
- **SBT**: 1.9.x
- **Java**: 11 or 17

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/ipl-analytics.git
cd ipl-analytics
```

### 2. Download IPL Dataset

Place your IPL JSON files in:
```bash
mkdir -p data/raw/ipl_json
# Copy your JSON files to data/raw/ipl_json/
```

**Dataset Source**: [cricsheet.org](https://cricsheet.org/downloads/)

### 3. Create Required Directories

```bash
mkdir -p output
mkdir -p notebooks
mkdir -p config
mkdir -p sql
```

### 4. Build the Scala Project

```bash
sbt clean compile package
# Output: target/scala-2.12/ipl-analytics_2.12-1.0.0.jar
```

### 5. Start the Services

```bash
docker-compose up -d
```

Verify all services are running:
```bash
docker-compose ps
```

Expected output: All services in "Up" state

### 6. Submit the Spark Job

```bash
# Copy JAR to Spark master
docker cp target/scala-2.12/ipl-analytics_2.12-1.0.0.jar ipl-spark-master:/opt/bitnami/spark/

# Submit the job
docker exec -it ipl-spark-master spark-submit \
  --class com.ipl.jobs.IPLAnalytics \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages org.postgresql:postgresql:42.7.1 \
  --driver-memory 3g \
  --executor-memory 2500m \
  --executor-cores 2 \
  --num-executors 4 \
  /opt/bitnami/spark/ipl-analytics_2.12-1.0.0.jar
```

## 📁 Project Structure

```
ipl-analytics/
├── src/
│   └── main/
│       └── scala/
│           └── com/
│               └── ipl/
│                   └── jobs/
│                       └── IPLAnalytics.scala
├── data/
│   └── raw/
│       └── ipl_json/              # JSON match data
├── output/                        # Parquet outputs
├── notebooks/                     # Jupyter notebooks
├── sql/
│   └── init.sql                   # PostgreSQL schema
├── config/                        # Spark configuration
├── build.sbt                      # SBT build configuration
├── docker-compose.yml             # Service orchestration
└── README.md
```

## 🎮 Usage

### Access Web UIs

| Service | URL | Description |
|---------|-----|-------------|
| Spark Master | http://localhost:8080 | Monitor Spark jobs and workers |
| Jupyter Notebook | http://localhost:8888 | Interactive data exploration |
| Kafka UI | http://localhost:8089 | Monitor Kafka topics |

### Query Results from PostgreSQL

```bash
docker exec -it ipl-postgres psql -U ipl_user -d ipl_analytics

# Example queries:
SELECT * FROM ipl_team_stats ORDER BY win_percentage DESC LIMIT 10;
SELECT * FROM ipl_player_batting_stats ORDER BY total_runs DESC LIMIT 10;
SELECT * FROM ipl_season_trends ORDER BY season;
```

### Access Parquet Files

```bash
# Files are in ./output/ directory
ls -lh output/

# Read with Spark in Jupyter:
# df = spark.read.parquet("/home/jovyan/work/output/team_stats")
```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file:

```bash
# Spark Configuration
SPARK_MASTER=spark://spark-master:7077
SPARK_WORKER_MEMORY=8G
SPARK_WORKER_CORES=4

# PostgreSQL
POSTGRES_HOST=ipl-postgres
POSTGRES_PORT=5432
POSTGRES_DB=ipl_analytics
POSTGRES_USER=ipl_user
POSTGRES_PASSWORD=ipl_password

# Redis
REDIS_HOST=ipl-redis
REDIS_PORT=6379

# Data Paths
IPL_DATA_PATH=/opt/bitnami/spark/data/ipl_json
IPL_OUTPUT_PATH=/opt/bitnami/spark/output
```

### Spark Configuration

Edit `config/spark-defaults.conf`:

```properties
spark.executor.memory              2500m
spark.driver.memory                3g
spark.sql.shuffle.partitions       200
spark.memory.fraction              0.8
spark.memory.storageFraction       0.3
spark.serializer                   org.apache.spark.serializer.KryoSerializer
```

### PostgreSQL Schema

Edit `sql/init.sql` for custom schema:

```sql
CREATE TABLE IF NOT EXISTS ipl_team_stats (
    season VARCHAR(10),
    team VARCHAR(100),
    matches_played INTEGER,
    matches_won INTEGER,
    win_percentage DECIMAL(5,2),
    PRIMARY KEY (season, team)
);
```

## 📚 API Documentation

### IPLAnalytics Class

#### Methods

**`loadIPLData(): DataFrame`**
- Loads IPL JSON files from configured path
- Returns: Raw DataFrame with match data
- Throws: RuntimeException if files not found

**`transformIPLData(rawDf: DataFrame): (DataFrame, DataFrame)`**
- Transforms raw data into matches and ball-by-ball DataFrames
- Parameters: rawDf - Raw match data
- Returns: Tuple of (matchesDf, ballsByBallDf)

**`calculateProductionStats(matchesDf: DataFrame, ballsDf: DataFrame): Map[String, DataFrame]`**
- Calculates all analytics statistics
- Parameters: Matches and balls DataFrames
- Returns: Map of statistic name to DataFrame

**`saveToPostgreSQL(dataFrames: Map[String, DataFrame]): Unit`**
- Saves DataFrames to PostgreSQL
- Falls back to Parquet if PostgreSQL unavailable
- Parameters: Map of table names to DataFrames

**`runProductionPipeline(): Unit`**
- Executes complete analytics pipeline
- Orchestrates all processing steps

## 🐛 Troubleshooting

### Common Issues

#### 1. "No resources available" Error

**Problem**: Workers don't have enough memory for executors

**Solution**:
```bash
# Increase worker memory in docker-compose.yml
SPARK_WORKER_MEMORY=8G

# Or reduce executor memory in submit:
--executor-memory 1500m
```

#### 2. "FileNotFoundException" for JSON files

**Problem**: Data path mismatch

**Solution**:
```bash
# Check files exist
docker exec ipl-spark-worker-1 ls -la /opt/bitnami/spark/data/ipl_json/

# Verify mount in docker-compose.yml:
- ./data/raw/ipl_json:/opt/bitnami/spark/data/ipl_json
```

#### 3. Out of Memory (OOM) Errors

**Problem**: Executors running out of memory

**Solution**:
```bash
# Increase worker memory
SPARK_WORKER_MEMORY=8G

# Reduce partitions in Scala code
spark.sql.shuffle.partitions=100
```

#### 4. Workers Not Connecting

**Problem**: Workers can't reach Spark master

**Solution**:
```bash
# Check network connectivity
docker exec ipl-spark-worker-1 ping spark-master

# Restart workers
docker-compose restart spark-worker-1 spark-worker-2
```

### Debug Commands

```bash
# View Spark master logs
docker logs ipl-spark-master -f

# View worker logs
docker logs ipl-spark-worker-1 -f

# Check PostgreSQL connection
docker exec -it ipl-postgres psql -U ipl_user -d ipl_analytics -c "\dt"

# Monitor resource usage
docker stats
```

## ⚡ Performance Tuning

### For Small Datasets (< 500 matches)

```bash
# docker-compose.yml
SPARK_WORKER_MEMORY=4G
SPARK_WORKER_CORES=2

# Submit command
--executor-memory 1500m
--num-executors 2
```

### For Medium Datasets (500-2000 matches)

```bash
# docker-compose.yml
SPARK_WORKER_MEMORY=8G
SPARK_WORKER_CORES=4

# Submit command
--executor-memory 2500m
--num-executors 4
```

### For Large Datasets (2000+ matches)

```bash
# docker-compose.yml
SPARK_WORKER_MEMORY=12G
SPARK_WORKER_CORES=6

# Submit command
--executor-memory 3500m
--num-executors 6
--conf spark.sql.shuffle.partitions=400
```

### Memory Optimization Tips

1. **Don't cache unless necessary** - Removed `.cache()` calls
2. **Increase shuffle partitions** for large datasets
3. **Use Kryo serialization** (already configured)
4. **Monitor Spark UI** for memory usage patterns
5. **Repartition** data before heavy operations

## 🤝 Contributing

Contributions are welcome! Please follow these guidelines:

### Development Setup

```bash
# Fork and clone
git clone https://github.com/yourusername/ipl-analytics.git
cd ipl-analytics

# Create feature branch
git checkout -b feature/your-feature

# Make changes and test
sbt test

# Commit with clear messages
git commit -m "Add: New venue analysis feature"

# Push and create PR
git push origin feature/your-feature
```

### Code Style

- Follow Scala style guide
- Add comments for complex logic
- Write unit tests for new features
- Update documentation

### Testing

```bash
# Run all tests
sbt test

# Run specific test
sbt "testOnly com.ipl.jobs.IPLAnalyticsSpec"
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Cricsheet** for providing IPL match data
- **Apache Spark** community for the amazing framework
- **Bitnami** for Docker images

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/ipl-analytics/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/ipl-analytics/discussions)
- **Email**: your.email@example.com

## 🗺️ Roadmap

- [ ] Add real-time streaming analytics with Kafka
- [ ] Implement machine learning models for match predictions
- [ ] Build REST API for analytics queries
- [ ] Add Grafana dashboards for visualization
- [ ] Support for other cricket leagues (BBL, CPL)
- [ ] Add player comparison features
- [ ] Implement data quality checks

## 📊 Sample Output

### Top Run Scorers
```
+------------------+----------+-----------+-------+
|batter            |total_runs|strike_rate|average|
+------------------+----------+-----------+-------+
|V Kohli           |7263      |130.41     |36.85  |
|SK Raina          |5528      |136.73     |32.51  |
|RG Sharma         |5879      |130.61     |31.17  |
+------------------+----------+-----------+-------+
```

### Team Performance
```
+------+-------------------------+--------------+-----------+--------------+
|season|team                     |matches_played|matches_won|win_percentage|
+------+-------------------------+--------------+-----------+--------------+
|2023  |Chennai Super Kings      |16            |10         |62.50         |
|2023  |Gujarat Titans           |16            |10         |62.50         |
|2023  |Mumbai Indians           |16            |9          |56.25         |
+------+-------------------------+--------------+-----------+--------------+
```

---

**Made with ❤️ for Cricket Analytics**

*Star ⭐ this repo if you find it useful!*
