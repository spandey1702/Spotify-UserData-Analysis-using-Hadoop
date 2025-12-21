## BigData Spotify Churn Analysis
A large-scale distributed data processing system for analyzing Spotify user behavior patterns and predicting churn using Hadoop MapReduce, Apache Pig, and Apache Hive.

## Overview
This project analyzes large-scale user interaction data to predict user churn (users who stop using the service) on Spotify using the Hadoop ecosystem. The system processes millions of user interactions using distributed computing to identify patterns, trends, and insights that inform retention strategies.
Problem Statement
Music streaming platforms face significant challenges in user retention:

Identifying users at risk of churning before they leave
Understanding regional music preferences for content localization
Recognizing high-value users for targeted engagement
Analyzing temporal trends in artist popularity
Optimizing platform features based on device usage patterns

## Solution
A distributed big data processing system that:
Predicts churn using interaction frequency and recency analysis
Identifies high-value users through genre diversity metrics
Analyzes regional preferences to inform content strategy
Ranks artists temporally using weighted engagement scores
Processes device analytics for platform optimization
Scales horizontally to handle massive datasets


## Architecture
Hadoop MapReduce Pipeline
Show Image
Data Processing Flow
Show Image
System Components
Data Layer:

HDFS - Distributed file system for storing CSV datasets
Input Data - User interactions and song metadata
Output Data - Analysis results stored in HDFS

## Processing Layer:

MapReduce Jobs - 5 Java-based distributed processing jobs
Apache Pig - Latin-based data transformation scripts
Apache Hive - SQL-like querying for data analysis

## Analysis Layer:

Churn Prediction - User retention analysis
User Segmentation - High-value user identification
Geographic Analysis - Regional preference mapping
Temporal Trends - Time-based popularity tracking
Engagement Metrics - Normalized user behavior patterns


Features
MapReduce Jobs
1. Churn Prediction

Analyzes user activity patterns to predict churn
Uses custom UserDeviceKey composite key for grouping
Calculates days since last interaction
Classification: Active (<200 days) vs Churned (>200 days)
Enables proactive retention campaigns

2. High-Value User Identification

Identifies users with diverse listening habits
Tracks unique genre interactions per user
Threshold: 4+ unique genres = High-value user
Uses HashSet for deduplication
Targets for premium subscription offers

3. Top Songs by Region

Analyzes geographic music preferences
Filters for "Play" actions only
Returns top 10 songs per country
Uses HashMap for frequency counting
Informs localized content strategy

4. Top Artists by Year

Temporal analysis of artist popularity
Weighted scoring: (2 × Play) - (1 × Skip)
Tracks year-over-year trends
Supports playlist curation decisions
Identifies emerging artists

5. Reduce-Side Join

Joins song metadata with user interactions
Two mappers: SongJoinMapper + InteractionJoinMapper
Custom CustomWritable for type differentiation
Complete user-song interaction records
Enables recommendation algorithms

6. User Engagement Analysis

Two-phase MapReduce pipeline
Phase 1: Aggregates interactions by subscription/action/genre/device
Phase 2: Normalizes scores for comparison
Identifies engagement patterns
Supports A/B testing analysis

Apache Pig Scripts
Device Analysis:

Most common device per subscription type
Groups by subscription and device
Informs platform development priorities

Age Demographics:

Average age by subscription tier
Supports targeted marketing campaigns

Playlist Creation:

Automated playlist classification
Categories: Workout, Party, Relax, Other
Based on audio features (tempo, danceability, energy)

Popularity Trends:

Tracks relationship between popularity and audio features
Identifies high-energy danceable songs
Genre-based popularity analysis


Technology Stack
Hadoop Ecosystem

Hadoop HDFS 3.x - Distributed file system for fault-tolerant storage
Hadoop MapReduce 3.x - Framework for parallel processing
Apache Pig 0.17 - Data flow language for transformations
Apache Hive 3.x - SQL-like querying (optional)

Programming Languages

Java 8 - MapReduce job development
Pig Latin - Data transformation scripts

Development Tools

Maven - Dependency management and build automation
IntelliJ IDEA / Eclipse - Java IDE
Hadoop CLI - Command-line tools for HDFS and job management

Installation
1. Clone the repository:
bashgit clone https://github.com/sagarikapandey17/BigData-Spotify-Churn-Analysis.git
cd BigData-Spotify-Churn-Analysis
2. Set up Hadoop:
bash# Start HDFS
start-dfs.sh

# Start YARN (for cluster mode)
start-yarn.sh

# Verify Hadoop is running
jps
# Should show: NameNode, DataNode, SecondaryNameNode
3. Create HDFS directories:
bashhdfs dfs -mkdir -p /spotify_data
hdfs dfs -mkdir -p /spotify_data/output
4. Upload datasets to HDFS:
bash# Upload user interactions
hdfs dfs -put synthetic_users_interactions.csv /spotify_data/

# Upload songs data
hdfs dfs -put synthetic_songs.csv /spotify_data/

# Verify upload
hdfs dfs -ls /spotify_data/

Running the Analysis
MapReduce Jobs
Churn Prediction:
bash# Compile Java classes
javac -classpath `hadoop classpath` -d churn_classes churnprediction/*.java

# Create JAR
jar -cvf churnanalysis.jar -C churn_classes/ .

# Run MapReduce job
hadoop jar churnanalysis.jar com.mycompany.churnprediction.ChurnAnalysisDriver \
  /spotify_data/synthetic_users_interactions.csv \
  /spotify_data/output/churn_analysis

# View results
hdfs dfs -cat /spotify_data/output/churn_analysis/part-r-00000 | head -20
High-Value Users:
bashjavac -classpath `hadoop classpath` -d highvalue_classes highvalueusers/*.java
jar -cvf highvalueusers.jar -C highvalue_classes/ .

hadoop jar highvalueusers.jar com.mycompany.highvalueusers.HighValueUserDriver \
  /spotify_data/synthetic_users_interactions.csv \
  /spotify_data/output/high_value_users

hdfs dfs -cat /spotify_data/output/high_value_users/part-r-00000
Top Songs by Region:
bashjavac -classpath `hadoop classpath` -d topsongs_classes TopSongsbyRegion/*.java
jar -cvf topsongs.jar -C topsongs_classes/ .

hadoop jar topsongs.jar TopSongsJob \
  /spotify_data/synthetic_users_interactions.csv \
  /spotify_data/output/top_songs_by_region

hdfs dfs -cat /spotify_data/output/top_songs_by_region/part-r-00000
Top Artists by Year:
bashjavac -classpath `hadoop classpath` -d topartist_classes topartist/*.java
jar -cvf topartist.jar -C topartist_classes/ .

hadoop jar topartist.jar com.mycompany.topartist.TopArtistJob \
  /spotify_data/synthetic_users_interactions.csv \
  /spotify_data/output/top_artists_by_year

hdfs dfs -cat /spotify_data/output/top_artists_by_year/part-r-00000
Reduce-Side Join:
bashjavac -classpath `hadoop classpath` -d join_classes playlist_recommender/*.java
jar -cvf reducejoin.jar -C join_classes/ .

hadoop jar reducejoin.jar com.mycompany.reducejoins.ReduceSideJoinDriver \
  /spotify_data/synthetic_songs.csv \
  /spotify_data/synthetic_users_interactions.csv \
  /spotify_data/output/joined_data

hdfs dfs -cat /spotify_data/output/joined_data/part-r-00000 | head -10
User Engagement Analysis:
bashjavac -classpath `hadoop classpath` -d engagement_classes userengagement_normalization\&aggregation/*.java
jar -cvf userengagement.jar -C engagement_classes/ .

hadoop jar userengagement.jar com.mycompany.userengagement_job1.InteractionAnalysisDriver \
  /spotify_data/synthetic_users_interactions.csv \
  /spotify_data/output/engagement_temp \
  /spotify_data/output/engagement_normalized

hdfs dfs -cat /spotify_data/output/engagement_normalized/part-r-00000
Apache Pig Scripts
Device Analysis:
bashpig -x mapreduce DeviceAnalysis.pig

# View output
hdfs dfs -cat /spotify_data/output/common_device_by_subscription/part-r-00000
Age Demographics:
bashpig -x mapreduce DevicebySubscription.pig

hdfs dfs -cat /spotify_data/output/avg_age_by_subscription/part-r-00000
Playlist Creation:
bashpig -x mapreduce playlist_creation.pig

# View workout playlist
hdfs dfs -cat /spotify_data/output/workout_playlist/part-r-00000 | head -20
Popularity Trends:
bashpig -x mapreduce TrendsAnalysisByPopularity.pig

hdfs dfs -cat /spotify_data/output/popularity_by_genre/part-r-00000
