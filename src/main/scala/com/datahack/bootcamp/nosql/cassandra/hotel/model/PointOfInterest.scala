package com.datahack.bootcamp.nosql.cassandra.hotel.model

case class PointOfInterest(idHotel: Int, name: String, category: String, distance: Float, address: String, lat: Float, long: Float) {
  override def toString: String = s"$idHotel,$name,$category,$distance,$address,$lat,$long\n"
}
