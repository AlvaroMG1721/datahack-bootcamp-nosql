package com.datahack.bootcamp.nosql.cassandra.hotel.model

case class Room(idHotel: Int, number: Int, price: Float, info: String) {
  override def toString: String = s"$idHotel,$number,$price,$info\n"
}
