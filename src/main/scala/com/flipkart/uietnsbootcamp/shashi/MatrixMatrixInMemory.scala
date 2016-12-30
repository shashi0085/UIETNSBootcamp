package com.flipkart.uietnsbootcamp.shashi

import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source

/**
  * Created by shashi.kushwaha on 30/12/16.
  * solves problem when one matrix can't fit in memory but other can
  */
object MatrixMatrixInMemory {
  def main(args: Array[String]):Unit = {
    val matrixFile = "src/main/scala/com/flipkart/uietnsbootcamp/shashi/matrix1"
    val matrixFile2 = "src/main/scala/com/flipkart/uietnsbootcamp/shashi/matrix2"


    val sparkConf = new SparkConf ().setAppName ("Matrix Vector Multiplication").setMaster("local")
    val sc = new SparkContext (sparkConf)

    val matrix1 = sc.textFile(matrixFile).zipWithIndex.flatMap(line => {
      val row = line._2
      line._1.split(",").zipWithIndex.map(word => {
        val col = word._2.toLong
        (row.toLong, col.toLong, word._1.toDouble)
      })
    })

    val read = Source.fromFile(matrixFile2).getLines().toList

    val matrix2 = read.zipWithIndex.flatMap(line => {
      val row = line._2
      line._1.split(",").zipWithIndex.map(word => {
        val col = word._2.toLong
        (row.toLong, col.toLong, word._1.toDouble)
      })
    })

    val bmatrix2 = sc.broadcast(matrix2)

    val result = matrix1.flatMap(touple1 => {
      bmatrix2.value.filter(touple2 => touple2._1 == touple1._2).map(touple2 => {
        ((touple1._1, touple2._2), touple1._3*touple2._3)
      })
    }).reduceByKey(_+_).collect()

    result.foreach(println)
  }
}
