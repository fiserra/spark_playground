package org.fiser.spark.playground

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)
     val result: RDD[(String, Int)] = sc.textFile("file:///D:/Dev/2008_2M.csv")
      .flatMap(line => line.split(","))
      .map(word => (word, 1))
      .reduceByKey { case (x, y) => x + y }

      result.collect().foreach(println)
  }

}
