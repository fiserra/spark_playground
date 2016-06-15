package org.fiser.spark.playground

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UnaryTransform {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val headerAndRows: RDD[Array[String]] = sc.textFile("file:///D:/Dev/2008_2M.csv")
      .map(line => line.split(",").map(line => line.trim))

    val header = headerAndRows.first
    // filter out header (eh. just check if the first val matches the first header name)
    val data: RDD[Array[String]] = headerAndRows.filter(_(0) != header(0))
    // splits to map (header/value pairs)
    val maps: RDD[FlightEvent] = data.map(splits => stringArrayToFlightEvent(splits))
    print(maps.filter(f => f.month.toInt == 1).count())
  }
}
