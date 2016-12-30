package com.flipkart.uietnsbootcamp.shashi

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by shashi.kushwaha on 30/12/16.
  */
object MatrixMatrixNoneFit {
  def main(args:Array[String]):Unit = {

    val matrixFile = "src/main/scala/com/flipkart/uietnsbootcamp/shashi/matrix1"
    val matrixFile2 = "src/main/scala/com/flipkart/uietnsbootcamp/shashi/matrix2"


    val sparkConf = new SparkConf().setAppName("Matrix Vector Multiplication").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val matrix1 = sc.textFile(matrixFile).zipWithIndex.flatMap(line => {
      val row = line._2
      line._1.split(",").zipWithIndex.map(word => {
        val col = word._2.toLong
        (col.toLong, (row.toLong, word._1.toDouble))
      })
    })


    val matrix2 = sc.textFile(matrixFile2).zipWithIndex.flatMap(line => {
      val row = line._2
      line._1.split(",").zipWithIndex.map(word => {
        val col = word._2.toLong
        (row.toLong, (col.toLong, word._1.toDouble))
      })
    })


    val result = matrix1.join(matrix2).map(entry => {
      ((entry._2._1._1, entry._2._2._1), entry._2._1._2 * entry._2._2._2)
    }).reduceByKey(_ + _).collect

    result.foreach(println)
  }

}
