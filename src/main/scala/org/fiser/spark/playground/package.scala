package org.fiser.spark

package object playground {
  def stringArrayToFlightEvent(cols: Array[String]): FlightEvent =
    new FlightEvent(cols(0), cols(1), cols(2), cols(3), cols(4), cols(5), cols(6), cols(7), cols(8), cols(9), cols(10),
      cols(11), cols(12), cols(13), cols(14), cols(15), cols(16), cols(17), cols(18), cols(19), cols(20), cols(21),
      cols(22), cols(23), cols(24), cols(25), cols(26), cols(27), cols(28))

  case class FlightEvent(year: String,
                         month: String,
                         dayOfMonth: String,
                         dayOfWeek: String,
                         depTime: String,
                         scheduledDepTime: String,
                         arrTime: String,
                         scheduledArrTime: String,
                         uniqueCarrier: String,
                         flightNum: String,
                         tailNum: String,
                         actualElapsedMins: String,
                         crsElapsedMins: String,
                         airMins: String,
                         arrDelayMins: String,
                         depDelayMins: String,
                         originAirportCode: String,
                         destinationAirportCode: String,
                         distanceInMiles: String,
                         taxiInTimeMins: String,
                         taxiOutTimeMins: String,
                         flightCancelled: String,
                         cancellationCode: String, // (A = carrier, B = weather, C = NAS, D = security)
                         diverted: String, // 1 = yes, 0 = no
                         carrierDelayMins: String,
                         weatherDelayMins: String,
                         nasDelayMins: String,
                         securityDelayMins: String,
                         lateAircraftDelayMins: String)

}
