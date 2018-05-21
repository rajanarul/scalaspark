package com.tryout.scalaspark

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf,SparkContext}



object MaxPrice {
  def main(args: Array[String]){

    println( "-------------------------------------------------" )
    args.foreach( arg => println(arg))
    println( "-------------------------------------------------" )


    val conf = new SparkConf().setMaster("local[2]").setAppName("Max Price")
    val sc = new SparkContext(conf)


    sc.textFile(args(0))
      .map(_.split(","))
      .map(rec => ((rec(0).split("-"))(0).toInt, rec(1).toFloat))
      .reduceByKey((a,b) => Math.max(a,b))
      .saveAsTextFile("output.txt")

  }
}
