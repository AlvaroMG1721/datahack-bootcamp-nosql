package com.datahack.bootcamp.nosql.cassandra.crud

import com.datahack.bootcamp.nosql.cassandra.crud.model.User
import com.datastax.driver.core.{ResultSet, ResultSetFuture, Session, Statement}
import com.datastax.driver.core.querybuilder.QueryBuilder

import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

class CrudWithAsyncQuery(session: Session) {

  val TABLE_NAME: String = "users"
  val NAME_COLUMN: String = "name"
  val EMAIL_COLUMN: String = "email"
  val COUNTRY_COLUMN: String = "country"
  val CITY_COLUMN: String = "city"

  def executeQuery(statement: Statement): Future[ResultSet] = ???

  def insertIntoUsers(keySpaceName: String, user: User): Future[Boolean] = {
    val query = QueryBuilder
      .insertInto(keySpaceName, TABLE_NAME)
      .value(NAME_COLUMN, user.name)
      .value(EMAIL_COLUMN, user.email)
      .value(COUNTRY_COLUMN, user.country)
      .value(CITY_COLUMN, user.city)
    executeQuery(query).map(_.isExhausted)
  }

  def getAllUsers(keySpaceName: String): Future[Seq[User]] = {
    import scala.collection.JavaConverters._
    val query = QueryBuilder.select.all().from(keySpaceName, TABLE_NAME)
    executeQuery(query)
      .map(resultSet => resultSet.asScala.map(User.fromRow).toSeq)
  }

  def getUserByName(keySpaceName: String, userName: String): Future[Option[User]] = {
    import scala.collection.JavaConverters._
    val query = QueryBuilder
      .select.all()
      .from(keySpaceName, TABLE_NAME)
      .where(QueryBuilder.eq(NAME_COLUMN, userName))
    executeQuery(query)
      .map(resultSet => resultSet.asScala.toSeq.headOption.map(User.fromRow))
  }

  def deleteUser(keySpaceName: String, userName: String): Future[Boolean] = {
    val query = QueryBuilder
      .delete()
      .from(keySpaceName, TABLE_NAME)
      .where(QueryBuilder.eq(NAME_COLUMN, userName))
    executeQuery(query).map(_.isExhausted)
  }

  def updateUser(keySpaceName: String, user: User): Future[Boolean] = {
    val query = QueryBuilder
      .update(keySpaceName, TABLE_NAME)
      .`with`(QueryBuilder.set(EMAIL_COLUMN, user.email))
      .where(QueryBuilder.eq(NAME_COLUMN, user.name))
      .and(QueryBuilder.eq(COUNTRY_COLUMN, user.country))
      .and(QueryBuilder.eq(CITY_COLUMN, user.city))
    executeQuery(query).map(_.isExhausted)
  }
}
