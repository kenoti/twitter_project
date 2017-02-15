package dsti

//Create table in Hive
/*
CREATE EXTERNAL TABLE tweets_hive
(
  id BIGINT, reply_status_id BIGINT,
  reply_user_id BIGINT,
  retweet_count INT,
  text STRING,
  latitude FLOAT,
  longitude FLOAT,
  source STRING,
  user_id INT,
  user_name STRING,
  user_screen_name STRING,
  user_created_at TIMESTAMP,
  user_followers BIGINT,
  user_favorites BIGINT,
  user_language STRING,
  user_location STRING,
  user_timezone STRING,
  created_at TIMESTAMP,
  created_at_year INT,
  created_at_month INT,
  created_at_day INT,
  created_at_hour INT,
  created_at_minute INT,
  created_at_second INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION 'hdfs://localhost:8020/user/hive/warehouse/tweets_hive';
 */

//Import classes needed in the project
import org.apache.spark.streaming._
//import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.twitter.TwitterUtils
import StreamingContext._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}

import scala.io.Source
import scala.io.Source
import twitter4j._

import java.util.Date

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.hadoop.io.compress.DefaultCodec
//import org.apache.spark.sql._
//import java.util.Calendars
import org.apache.spark.streaming.Duration

//import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * @author ${user.name}
 */
object Streamer  extends App{

  // Your code here
  @transient var sc: SparkContext = _

  /**
   * Load twitter oauth keys for twitter4j client from twitter4j.properties
   * It should contain the following keys:
   *
   * twitter4j.oauth.consumerKey=..
   * twitter4j.oauth.consumerSecret=..
   * twitter4j.oauth.accessToken=..
   * twitter4j.oauth.accessTokenSecret=..
   *
   */
  def loadTwitterKeys() = {
    //val lines: Iterator[String] = Source.fromFile("twitter4j.properties").getLines()
    System.setProperty("twitter4j.oauth.consumerKey", "")
    System.setProperty("twitter4j.oauth.consumerSecret", "")
    System.setProperty("twitter4j.oauth.accessToken", "")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "")
    //val props = lines.map(line => line.split("=")).map { case (scala.Array(k, v)) => (k, v)}
    //props.foreach {
    //  case (k: String, v: String) => System.setProperty(k, v)
    //}
  }

  /**
   * Function to create streaming context and set refresh to 60 sec
   * @return
   */
  def configureStreamingContext() = {
    //val conf = new SparkConf().setMaster("local[3]").setAppName("Twitter")
      val conf = new SparkConf().setAppName("Twitter")
    sc = new SparkContext(conf)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    new StreamingContext(sc, Seconds(60))
  }



  /**
   * Start the stream using filter keywords and print reduced results every 60 seconds
   */
  def startStream() = {
    val duration: Duration = Seconds(3600)
    val filters = Seq("football")

    val ssc = configureStreamingContext()
    //filter enqlish tweets about football
    val tweets = TwitterUtils.createStream(ssc, None, filters, StorageLevel.MEMORY_ONLY_SER_2).filter(_.getLang() == "en").filter(_.getUser.getLang == "en")
    //val tweets = TwitterUtils.createStream(ssc, None).filter(_.getUser().getLang() == "en")


    // Print tweets batch count
    tweets.foreachRDD(rdd => {
      println("\nNew tweets %s:".format(rdd.count()))
    })

    //Writing Twitter Streams as json files
    val englishTweets = tweets.filter(_.getLang() == "en")
    englishTweets.saveAsTextFiles("hdfs://localhost:8020/user/hive/warehouse/tweets/tweets", "json")

    //End Writing Twitter Streams

    val statuses = tweets.map(status => status.getText())

    statuses.foreachRDD( rdd => {
      for(item <- rdd.collect().toArray) {
        println(item);
      }
    })



    //get users and followers count
    val users = tweets.map(status =>
      (status.getUser().getScreenName(), status.getUser().getFollowersCount())
    )

    //print top users
    val usersReduced = users.reduceByKeyAndWindow(_ + _, duration).map { case (user, count) => (count, user)}.transform(_.sortByKey(false))
    usersReduced.foreachRDD(rdd => {
      println("ReducedUsersCount= %s ".format(rdd.count()))
      val topUsers = rdd.take(100)
      topUsers.foreach { case (count, user) => println("%s (%s followers)".format(user, count))}
    })

    // Print popular hash tags
    val hashTags = tweets.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    val topHashTags = hashTags.map((_, 1))
      .reduceByKeyAndWindow(_ + _, duration)
      .map { case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    topHashTags.foreachRDD(rdd => {
      val topList = rdd.take(100)
      println("\nPopular topics in last %s seconds (%s total):".format(duration, rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    //Hive writing to table
    // Local directory for stream checkpointing (allows us to restart this stream on failure)
    val checkpointDir = sys.env.getOrElse("CHECKPOINT_DIR", "/tmp/tweets/").toString

    // Output directory
    val outputDir = sys.env.getOrElse("OUTPUT_DIR", "hdfs://localhost:8020/user/hive/warehouse/tweets_hive")

    // Size of output batches in seconds
    val outputBatchInterval = sys.env.get("OUTPUT_BATCH_INTERVAL").map(_.toInt).getOrElse(60)

    // Number of output files per batch interval.
    val outputFiles = sys.env.get("OUTPUT_FILES").map(_.toInt).getOrElse(1)

    // Echo settings to the user
    Seq(("CHECKPOINT_DIR" -> checkpointDir),
      ("OUTPUT_DIR" -> outputDir),
      ("OUTPUT_FILES" -> outputFiles),
      ("OUTPUT_BATCH_INTERVAL" -> outputBatchInterval)).foreach {
      case (k, v) => println("%s: %s".format(k, v))
    }

    outputBatchInterval match {
      case 3600 =>
      case 60 =>
      case _ => throw new Exception(
        "Output batch interval can only be 60 or 3600 due to Hive partitioning restrictions.")
    }

    val hiveDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0")
    val year = new java.text.SimpleDateFormat("yyyy")
    val month = new java.text.SimpleDateFormat("MM")
    val day = new java.text.SimpleDateFormat("dd")
    val hour = new java.text.SimpleDateFormat("HH")
    val minute = new java.text.SimpleDateFormat("mm")
    val second = new java.text.SimpleDateFormat("ss")

    // A list of fields we want along with Hive column names and data types
    val fields: Seq[(Status => Any, String, String)] = Seq(
      (s => s.getId, "id", "BIGINT"),
      (s => s.getInReplyToStatusId, "reply_status_id", "BIGINT"),
      (s => s.getInReplyToUserId, "reply_user_id", "BIGINT"),
      (s => s.getRetweetCount, "retweet_count", "INT"),
      (s => s.getText, "text", "STRING"),
      (s => Option(s.getGeoLocation).map(_.getLatitude()).getOrElse(""), "latitude", "FLOAT"),
      (s => Option(s.getGeoLocation).map(_.getLongitude()).getOrElse(""), "longitude", "FLOAT"),
      (s => s.getSource, "source", "STRING"),
      (s => s.getUser.getId, "user_id", "INT"),
      (s => s.getUser.getName, "user_name", "STRING"),
      (s => s.getUser.getScreenName, "user_screen_name", "STRING"),
      (s => hiveDateFormat.format(s.getUser.getCreatedAt), "user_created_at", "TIMESTAMP"),
      (s => s.getUser.getFollowersCount, "user_followers", "BIGINT"),
      (s => s.getUser.getFavouritesCount, "user_favorites", "BIGINT"),
      (s => s.getUser.getLang, "user_language", "STRING"),
      (s => s.getUser.getLocation, "user_location", "STRING"),
      (s => s.getUser.getTimeZone, "user_timezone", "STRING"),

      // Break out date fields for partitioning
      (s => hiveDateFormat.format(s.getCreatedAt), "created_at", "TIMESTAMP"),
      (s => year.format(s.getCreatedAt), "created_at_year", "INT"),
      (s => month.format(s.getCreatedAt), "created_at_month", "INT"),
      (s => day.format(s.getCreatedAt), "created_at_day", "INT"),
      (s => hour.format(s.getCreatedAt), "created_at_hour", "INT"),
      (s => minute.format(s.getCreatedAt), "created_at_minute", "INT"),
      (s => second.format(s.getCreatedAt), "created_at_second", "INT")
    )

    // For making a table later, print out the schema
    val tableSchema = fields.map{case (f, name, hiveType) => "%s %s".format(name, hiveType)}.mkString("(", ", ", ")")
    println("Beginning collection. Table schema for Hive is: %s".format(tableSchema))

    println("Print format Status")
    // Remove special characters inside of statuses that screw up Hive's scanner.
    def formatStatus(s: Status): String = {
      def safeValue(a: Any) = Option(a)
        .map(_.toString)
        .map(_.replace("\t", ""))
        .map(_.replace("\"", ""))
        .map(_.replace("\n", ""))
        .map(_.replaceAll("[\\p{C}]","")) // Control characters
        .getOrElse("")

      fields.map{case (f, name, hiveType) => f(s)}
        .map(f => safeValue(f))
        .mkString("\t")
    }

    // Date format for creating Hive partitions
    val outDateFormat = outputBatchInterval match {
      case 60 => new java.text.SimpleDateFormat("yyyy/MM/dd/HH/mm")
      case 3600 => new java.text.SimpleDateFormat("yyyy/MM/dd/HH")
    }

    /** Spark stream declaration */

    // New Twitter stream
    val statusesStream = tweets

    // Format each tweet
    val formattedStatuses = statusesStream.map(s => formatStatus(s))

    // Group into larger batches
    val batchedStatuses = formattedStatuses.window(Seconds(outputBatchInterval), Seconds(outputBatchInterval))

    // Coalesce each batch into fixed number of files
    val coalesced = batchedStatuses.transform(rdd => rdd.coalesce(outputFiles))

    // Save as output in correct directory
    coalesced.foreachRDD((rdd, time) =>  {
      val outPartitionFolder = outDateFormat.format(new Date(time.milliseconds))
      //rdd.saveAsTextFile("%s/%s".format(outputDir, outPartitionFolder), classOf[DefaultCodec])
      rdd.saveAsTextFile(outputDir)
    })
    //End of Hive Insert

    ssc.start()
    //Time out your streaming after 3 minutes to prevent endless loop
    //ssc.awaitTermination(Minutes(3).milliseconds)
    println("Print Awaiting Termination of Stream")
    //ssc.awaitTerminationOrTimeout(10000)
    ssc.awaitTerminationOrTimeout(Minutes(10).milliseconds)
    //ssc.awaitTermination()
    ssc.stop(true)

    if (sc != null) {
      sc.stop()
    }
  }

  override def main(args: Array[String]) {
    println("Hello World ! ")

    // Your code here
    //Set log level
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)
    //Set the Twitter key configuration to allow you to log into twitter
    Streamer.loadTwitterKeys()
    //Start Streaming
    Streamer.startStream()

    /*
    //Script to check data in hive
    val conf = new SparkConf().setMaster("local[3]").setAppName("Load From Hive")
    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val hiveContext = new HiveContext(sc)
    System.out.println("INFO: ****************** Starting Connection HIVE ******************")
    //hiveContext.sql("use default")
    hiveContext.sql("SELECT * from tweets_hive").collect().foreach(rdd => println(rdd))
    System.out.println("INFO: ****************** Connected in HIVE ******************")
    System.out.println("INFO: ****************** End Test Scala ******************")
     */

  }

}