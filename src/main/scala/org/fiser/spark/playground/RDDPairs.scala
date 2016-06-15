package org.fiser.spark.playground

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

object RDDPairs {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]").set("spark.driver.memory", "2g")
    val sc = new SparkContext(conf)
    val headerAndRows: RDD[Array[String]] = sc.textFile("file:///D:/Dev/2008_2M.csv")
      .map(line => line.split(",").map(line => line.trim))

    val header = headerAndRows.first
    // filter out header (eh. just check if the first val matches the first header name)
    val noHeader = headerAndRows.filter(_ (0) != header(0))
    // splits to map (header/value pairs)

    val flightEventRDD: RDD[FlightEvent] = noHeader.map(array => stringArrayToFlightEvent(array)).cache()


    val daysMap: Seq[(String, Long)] = flightEventRDD.map(f => (f.dayOfWeek, f)).countByKey().toSeq.sortBy(_._2)
    println(Map)

    val nrOfAirplanes = flightEventRDD.keyBy(f => f.tailNum).keys.distinct().count()
    println("Nr of airplanes used:" + nrOfAirplanes)
  }
}
