package com.datahack.bootcamp.nosql.cassandra.crud

import com.datahack.bootcamp.nosql.cassandra.crud.model.User
import com.datastax.driver.core.{Cluster, ResultSet, Row, Session}

import scala.util.Try

class Crud(session: Session) {

  def createKeySpace(keySpaceName: String, replicationStrategy: String, replicationFactor: Int): Try[Boolean] = ???

  def dropKeySpace(keySpaceName: String): Try[Boolean] = ???

  def createTableUsers(keySpaceName: String): Try[Boolean] = ???

  def insertIntoUsers(keySpaceName: String, user: User): Try[Boolean] = ???

  def getAllUsers(keySpaceName: String): Seq[User] = {
    import scala.collection.JavaConverters._
    val result: ResultSet = ???
    result.asScala.map(User.fromRow).toSeq
  }

  def getUserByName(keySpaceName: String, userName: String): Option[User] = ???

  def deleteUser(keySpaceName: String, userName: String): Try[Boolean] = ???

  def updateUser(keySpaceName: String, user: User): Try[Boolean] = ???

}
