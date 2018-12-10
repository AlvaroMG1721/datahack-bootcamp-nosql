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

  def insertIntoUsers(keySpaceName: String, user: User): Try[Boolean] = {
    Try {
      val query = QueryBuilder
        .insertInto(keySpaceName, TABLE_NAME)
        .value(NAME_COLUMN, user.name)
        .value(EMAIL_COLUMN, user.email)
        .value(COUNTRY_COLUMN, user.country)
        .value(CITY_COLUMN, user.city)
      session.execute(query)
    } map(_.isExhausted)
  }

  def getAllUsers(keySpaceName: String): Seq[User] = {
    import scala.collection.JavaConverters._
    val query = QueryBuilder.select.all().from(keySpaceName, TABLE_NAME)
    val result = session.execute(query)
    result.asScala.map(User.fromRow).toSeq
  }

  def getUserByName(keySpaceName: String, userName: String): Option[User] = {
    import scala.collection.JavaConverters._
    val query = QueryBuilder
      .select.all()
      .from(keySpaceName, TABLE_NAME)
      .where(QueryBuilder.eq(NAME_COLUMN, userName))
    val result = session.execute(query)
    result.iterator().asScala.toSeq.headOption.map(User.fromRow)
  }

  def deleteUser(keySpaceName: String, userName: String): Try[Boolean] = {
    Try {
      val query = QueryBuilder
        .delete()
        .from(keySpaceName, TABLE_NAME)
        .where(QueryBuilder.eq(NAME_COLUMN, userName))
      session.execute(query)
    } map(_.isExhausted)
  }

  def updateUser(keySpaceName: String, user: User): Try[Boolean] = {
    Try {
      val query = QueryBuilder
        .update(keySpaceName, TABLE_NAME)
        .`with`(QueryBuilder.set(EMAIL_COLUMN, user.email))
        .where(QueryBuilder.eq(NAME_COLUMN, user.name))
        .and(QueryBuilder.eq(COUNTRY_COLUMN, user.country))
        .and(QueryBuilder.eq(CITY_COLUMN, user.city))
      session.execute(query)
    } map(_.isExhausted) recover {case e: Exception => e.printStackTrace(); true}
  }
}
