package org.fiser.spark.playground

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

object Action {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val headerAndRows: RDD[Array[String]] = sc.textFile("file:///D:/Dev/2008.csv")
      .map(line => line.split(",").map(line => line.trim))

    val header = headerAndRows.first
    // filter out header (eh. just check if the first val matches the first header name)
    val noHeader = headerAndRows.filter(_(0) != header(0))
    // splits to map (header/value pairs)
    val flightEventRDD: RDD[FlightEvent] = noHeader.map(array => stringArrayToFlightEvent(array))

    flightEventRDD.take(5).foreach(println)

    val DEN_to_LAS = flightEventRDD.filter(fe => fe.originAirportCode == "DEN" &&
                                                 fe.destinationAirportCode == "LAS" &&
                                                 fe.month.toInt == 3).cache( )

    //collect
    DEN_to_LAS.collect().foreach(println)

    //foreach
    val totalDelay  = sc.accumulator(0L)
    val totalFlights = sc.accumulator(0L)
    DEN_to_LAS.foreach{ f =>
      totalDelay += Try(f.arrDelayMins.toLong).getOrElse(0l) + Try(f.depDelayMins.toLong).getOrElse(0l)
      totalFlights += 1
    }
    println(s"Average delay time ${totalDelay.value/ totalFlights.value}")

    //reduce
    val totalDistance = DEN_to_LAS.map(f => Try(f.distanceInMiles.toLong).getOrElse(0l)).reduce((x, y) => x + y)
    println(s"Total distance: $totalDistance")

    //save to file
    DEN_to_LAS.saveAsTextFile("file:///D:/Dev/DEN_LAS_JAN.csv")
  }
}
