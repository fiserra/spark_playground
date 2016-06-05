package org.fiser.spark.playground

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Parallelize {

  def main(args: Array[String]): Unit = {
    val list = List(("Radu", 3000), ("Ion", 3004), ("Vasile", 200), ("Ion", 2000))
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.parallelize(list)

    rdd.count()
  }
}
