package com.flipkart.uietnsbootcamp.shashi

/**
  * Created by shashi.kushwaha on 27/12/16.
  */

import org.apache.spark._
import org.apache.spark.streaming.twitter.TwitterUtils


import com.google.gson.Gson
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object PiThingy {
  val conf = new SparkConf().setAppName("myApp").setMaster("local")
  val sc = new SparkContext(conf)
  def main(args: Array[String]) = {
    val file = sc.textFile("/Users/shashi.kushwaha/Second.scala")
    val words = file.flatMap(line => line.split(" ")).map(word => (word, 1))
    val counts = words.reduceByKey(_+_);
    counts.saveAsTextFile("/Users/shashi.kushwaha/save")
  }

}
