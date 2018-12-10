package com.datahack.bootcamp.nosql.cassandra.hotel.model

case class Hotel(id: Int, name: String, city: String, country: String, category: Int, lat: Float, long: Float) {
  override def toString: String = s"$id,$name,$city,$country,$category,$lat,$long\n"
}
