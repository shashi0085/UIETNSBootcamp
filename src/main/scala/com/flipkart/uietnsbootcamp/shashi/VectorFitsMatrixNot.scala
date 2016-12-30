package com.flipkart.uietnsbootcamp.shashi

import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source

/**
  * Created by shashi.kushwaha on 30/12/16.
  * solves problem when matrix can't fit in memory but vector does
  */
object VectorFitsMatrixNot {
  def main(args: Array[String]): Unit = {
    val matrixFile = "src/main/scala/com/flipkart/uietnsbootcamp/shashi/matrix"
    val vectorFile = "src/main/scala/com/flipkart/uietnsbootcamp/shashi/vector"
    val sparkConf = new SparkConf ().setAppName ("Matrix Vector Multiplication").setMaster("local")
    val sc = new SparkContext (sparkConf)
    val read = Source.fromFile(vectorFile).getLines().toList
    val vector = read.flatMap(entry => entry.split(",").zipWithIndex.map(entry => (entry._2.toLong, entry._1.toDouble))).toMap
    println(vector)

    val bvector = sc.broadcast(vector)
    val matrix = sc.textFile(matrixFile).zipWithIndex.flatMap(line => {
      val row = line._2
      line._1.split(",").zipWithIndex.map(word => {
        val col = word._2.toLong
        (row.toLong, col.toLong, word._1.toDouble)
      })
    })
    val toPrint = matrix.collect
    toPrint.foreach(println)

    val mapMatrix =  matrix.map(entry => {
      (entry._1,entry._3 * bvector.value.get(entry._2).getOrElse(0.0))
    })

    val result = mapMatrix.reduceByKey(_+_).collect().toList

    println(result)
  }
}
