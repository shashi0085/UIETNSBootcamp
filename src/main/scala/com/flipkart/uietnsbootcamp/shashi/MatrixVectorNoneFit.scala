package com.flipkart.uietnsbootcamp.shashi

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by shashi.kushwaha on 30/12/16.
  * solves problem when neither matrix nor vector fits in memory
  */
object MatrixVectorNoneFit {
  def main(args:Array[String]):Unit = {
    val sparkConf = new SparkConf().setAppName("Twitter Streaming").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val matrixFile = "src/main/scala/com/flipkart/uietnsbootcamp/shashi/matrix"
    val vectorFile = "src/main/scala/com/flipkart/uietnsbootcamp/shashi/vector"
    //val vector =
    val matrix = sc.textFile(matrixFile)
    val matrixRdd = matrix.zipWithIndex.flatMap(line => {
      val row = line._2
      line._1.split(",").zipWithIndex.map(word => {
        val col = word._2.toLong
        (col, (row, word._1.toDouble))
      })
    })

    val vector = sc.textFile(vectorFile).flatMap(entry => entry.split(",").zipWithIndex.map(entry => (entry._2.toLong, entry._1.toDouble)))
    val mapper = matrixRdd.join(vector).map(entry => {
      val key = entry._2._1._1
      val value = entry._2._1._2 * entry._2._2
      (key, value)
    })
    val result = mapper.reduceByKey(_+_).sortByKey(true)
    result.collect.foreach(println)
  }
}

