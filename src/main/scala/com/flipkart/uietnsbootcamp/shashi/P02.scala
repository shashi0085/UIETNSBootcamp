package com.flipkart.uietnsbootcamp.shashi

import org.apache.spark.streaming._
import scala.io.Source
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.AuthorizationFactory
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io._

/**
  * Created by shashi.kushwaha on 29/12/16.
  */
object P02 {
  val sparkConf = new SparkConf().setAppName("Twitter Streaming").setMaster("local")
  val sc = new SparkContext(sparkConf)
  val matrixFile = "src/main/scala/com/flipkart/uietnsbootcamp/shashi/matrix"
  val vectorFile = "src/main/scala/com/flipkart/uietnsbootcamp/shashi/vector"
  //val vector =
  val matrix = sc.textFile(matrixFile)
  val matrixRdd = matrix.zipWithIndex.flatMap(line =>{
    val row = line._2
    line._1.split(",").zipWithIndex.map(word =>{
      val col = word._2.toLong
      (col, (row, word._1.toDouble))
    })
  })

  val vector = Source.fromFile(vectorFile).getLines().toList.head.split(",").zipWithIndex.map(entry => (entry._2.toLong, 0L,entry._1.toDouble)).toList
  val bVector = sc.broadcast(vector)
  val byColumnMatrix = matrix.map(entry =>{
    val rowCol = matrixRdd.groupByKey()
  })




}
