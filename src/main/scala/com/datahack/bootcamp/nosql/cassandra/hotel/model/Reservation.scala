package com.datahack.bootcamp.nosql.cassandra.hotel.model

case class Reservation(idHotel: Int, roomNumber: Int, free: Boolean, date: String) {
  override def toString: String = s"$idHotel,$roomNumber,$free,$date\n"
}
