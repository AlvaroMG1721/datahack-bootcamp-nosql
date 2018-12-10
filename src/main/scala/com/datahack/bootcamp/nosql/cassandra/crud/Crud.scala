package com.datahack.bootcamp.nosql.cassandra.crud

import com.datahack.bootcamp.nosql.cassandra.crud.model.User
import com.datastax.driver.core.{Cluster, ResultSet, Row, Session}

import scala.util.Try

class Crud(session: Session) {

  def createKeySpace(keySpaceName: String, replicationStrategy: String, replicationFactor: Int): Try[Boolean] = {
    Try(session.execute(
      s"""
         |CREATE KEYSPACE $keySpaceName
         |WITH replication = {'class':'$replicationStrategy', 'replication_factor':$replicationFactor}
         |""".stripMargin)
    ).map(_.isExhausted)
  }

  def dropKeySpace(keySpaceName: String): Try[Boolean] = {
    Try(session.execute(
      s"""
         |DROP KEYSPACE IF EXISTS $keySpaceName
       """.stripMargin)
    ).map(_.isExhausted)
  }

  def createTableUsers(keySpaceName: String): Try[Boolean] = {
    Try(session.execute(
      s"""
         |CREATE TABLE $keySpaceName.users (
         |    name text,
         |    email text,
         |    city text,
         |    country text,
         |    PRIMARY KEY (name, country, city)
         |)
       """.stripMargin)
    ).map(_.isExhausted)
  }

  def insertIntoUsers(keySpaceName: String, user: User): Try[Boolean] = {
    Try(session.execute(
      s"""
         |INSERT INTO $keySpaceName.users (name, email, city, country)
         |VALUES ('${user.name}', '${user.email}', '${user.city}', '${user.country}')
      """.stripMargin)
    ).map(_.isExhausted)
  }

  def getAllUsers(keySpaceName: String): Seq[User] = {
    import scala.collection.JavaConverters._
    val result = session.execute(
      s"""
         |SELECT * FROM $keySpaceName.users
       """.stripMargin)
    result.asScala.map(User.fromRow).toSeq
  }

  def getUserByName(keySpaceName: String, userName: String): Option[User] = {
    import scala.collection.JavaConverters._
    val result = session.execute(
      s"""
         |SELECT * FROM $keySpaceName.users WHERE name = '$userName'
       """.stripMargin)
    result.iterator().asScala.toSeq.headOption.map(User.fromRow)
  }

  def deleteUser(keySpaceName: String, userName: String): Try[Boolean] = {
    Try(session.execute(
      s"""
         |DELETE FROM $keySpaceName.users WHERE name = '$userName'
       """.stripMargin)
    ).map(_.isExhausted)
  }

  def updateUser(keySpaceName: String, user: User): Try[Boolean] = {
    Try(session.execute(
      s"""
         |UPDATE $keySpaceName.users
         |SET email = '${user.email}'
         |WHERE name = '${user.name}' AND country = '${user.country}' AND city = '${user.city}'
       """.stripMargin)
    ).map(_.isExhausted)
  }

}
