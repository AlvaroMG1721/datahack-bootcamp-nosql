package com.datahack.bootcamp.nosql.cassandra.crud.model

import com.datastax.driver.core.Row

case class User(name: String, email: String, city: String, country: String)

object User {

  def fromRow(row: Row): User =
    User(
      name = row.getString("name"),
      email = row.getString("email"),
      city = row.getString("city"),
      country = row.getString("country")
    )
}