package com.flipkart.uietnsbootcamp.shashi

/**
  * Created by shashi.kushwaha on 27/12/16.
  */

import org.apache.spark._
import org.apache.spark.streaming._
import scala.io.Source
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.StreamingContext._
import twitter4j.auth.Authorization
import twitter4j.Status
import twitter4j.auth.AuthorizationFactory
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.streaming.api.java.JavaStreamingContext

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.Function
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream
import java.io._

object TwitterStreaming {

  def main(args: Array[String]) = {


    /*val consumerKey = "rLPyAEJMI4UvxxsR6Raf2u87u"
    val consumerSecret = "VCyGGFLWZ1u1Xxuk46Pzk0sAlM45vyxc0lsKcNfRbcp7FGUIwa"
    val accessToken = "714072919-eXRQbv72W9QypNhCg14bUhZurA6cIy3X53QUzZ7I"
    val accessTokenSecret = "iwomxzXjH9i2mUDcxaVCy0mSFBFp1c5vBffkKdjqw69fi"*/

    val file = Source.fromFile("src/main/scala/com/flipkart/uietnsbootcamp/shashi/twitter.txt").getLines().toList //Used to read file and ocn
    val keys = file.map(line => line.split("=").tail.head)
    val consumerKey = keys(0)
    val consumerSecret = keys(1)
    val accessToken = keys(2)
    val accessTokenSecret = keys(3)

    val url = "https://stream.twitter.com/1.1/statuses/filter.json"

    val sparkConf = new SparkConf().setAppName("Twitter Streaming").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val documents: RDD[Seq[String]] = sc.textFile("").map(_.split(" ").toSeq)


    // Twitter Streaming
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("/Users/shashi.kushwaha/flipkart/checkpoint") //used for fault tolerence

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
      rdd.repartition(1).saveAsTextFile("/Users/shashi.kushwaha/flipkart/streaming/streaming_" + i) //saving the RDDs given by window
      i += 1;
    })

    val sortedCounts = counts.map { case(tag, count) => (count, tag) } //making count as key.
      .transform(rdd => rdd.sortByKey(false)) //sorting the RDD by key, descending order
    var j = 0;
    sortedCounts.foreachRDD{rdd =>
      val writer = new FileWriter(new File("/Users/shashi.kushwaha/flipkart/top/top_"+j))
      writer.write(rdd.top(10).mkString("\n")) //taking the top 10 key-value pairs and saving them in file.
      writer.close()
     // sc.parallelize(rdd.top(10)).saveAsTextFile("/Users/shashi.kushwaha/flipkart/top/top_"+j)
        j += 1;}

    ssc.start()
    ssc.awaitTermination()
  }
}