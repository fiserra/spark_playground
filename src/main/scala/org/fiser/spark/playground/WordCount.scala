package org.fiser.spark.playground

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.textFile("file:///D:/Dev/2008.csv")
      .flatMap(line => line.split(","))
      .map(word => (word, 1))
      .reduceByKey { case (x, y) => x + y }
      .collect()
      .foreach(println)
  }

}
