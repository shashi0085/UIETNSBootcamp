package com.flipkart.uietnsbootcamp.shashi

/**
  * Created by shashi.kushwaha on 27/12/16.
  */

import org.apache.spark.streaming._
import scala.io.Source
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.AuthorizationFactory
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io._

object TwitterStreaming {

  def main(args: Array[String]) = {
    val twitterKeysFile = "src/main/scala/com/flipkart/uietnsbootcamp/shashi/twitter.txt"
    val url = "https://stream.twitter.com/1.1/statuses/filter.json"
    val checkPointFile = "/Users/shashi.kushwaha/flipkart/checkpoint"
    val hashtagFilePrefix = "/Users/shashi.kushwaha/flipkart/streaming/streaming_"
    val topTagsFilePrefix = "/Users/shashi.kushwaha/flipkart/top/top_"

    val file = Source.fromFile(twitterKeysFile).getLines().toList //Used to read file and ocn
    val keys = file.map(line => line.split("=").tail.head)
    val consumerKey = keys(0)
    val consumerSecret = keys(1)
    val accessToken = keys(2)
    val accessTokenSecret = keys(3)

    val sparkConf = new SparkConf().setAppName("Twitter Streaming").setMaster("local")
    val sc = new SparkContext(sparkConf)

    // Twitter Streaming
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint(checkPointFile) //used for fault tolerence

    val conf = new ConfigurationBuilder()
    conf.setOAuthAccessToken(accessToken)
    conf.setOAuthAccessTokenSecret(accessTokenSecret)
    conf.setOAuthConsumerKey(consumerKey)
    conf.setOAuthConsumerSecret(consumerSecret)
    conf.setStreamBaseURL(url)
    conf.setSiteStreamBaseURL(url) //making authorization conf for twitter access

    val filter = Array("Twitter", "Hadoop", "Big Data")

    val auth = AuthorizationFactory.getInstance(conf.build()) //building authorization
    val tweets = TwitterUtils.createStream(ssc, Some(auth), filter) //creating stream to receive data from twitter
    var i = 0;
    val statuses = tweets.map(status => status.getText)
    val words = statuses.flatMap(status => status.split(" ")) //splitting tweets to get words
    val hashtags = words.filter(word => word.startsWith("#")) //getting hash tags

    val counts = hashtags.map(tag => (tag, 1))
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(60 * 5), Seconds(2)) //reducing by window of size 5 minutes and after every two seconds
    //the window will be moved forward. we deduct the count of data going out and adds the new one.

    counts.foreachRDD(rdd =>{
      rdd.repartition(1).saveAsTextFile( hashtagFilePrefix + i) //saving the RDDs given by window
      i += 1
    })

    val sortedCounts = counts.map { case(tag, count) => (count, tag) } //making count as key.
      .transform(rdd => rdd.sortByKey(false)) //sorting the RDD by key, descending order
    var j = 0;
    sortedCounts.foreachRDD{rdd =>
      val writer = new FileWriter(new File(topTagsFilePrefix+j))
      writer.write(rdd.top(10).mkString("\n")) //taking the top 10 key-value pairs and saving them in file.
      writer.close()
        j += 1}

    ssc.start()
    ssc.awaitTermination()
  }
}