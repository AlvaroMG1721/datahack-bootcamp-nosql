package com.datahack.bootcamp.nosql.cassandra.crud

import com.datahack.bootcamp.nosql.cassandra.crud.model.User
import com.datastax.driver.core.Session
import com.datastax.driver.core.querybuilder.{Clause, QueryBuilder}

import scala.util.Try

class CrudWithQueryBuilder(session: Session) {

  val TABLE_NAME: String = "users"
  val NAME_COLUMN: String = "name"
  val EMAIL_COLUMN: String = "email"
  val COUNTRY_COLUMN: String = "country"
  val CITY_COLUMN: String = "city"

  def insertIntoUsers(keySpaceName: String, user: User): Try[Boolean] = ???

  def getAllUsers(keySpaceName: String): Seq[User] = ???

  def getUserByName(keySpaceName: String, userName: String): Option[User] = ???

  def deleteUser(keySpaceName: String, userName: String): Try[Boolean] = ???

  def updateUser(keySpaceName: String, user: User): Try[Boolean] = ???
}
