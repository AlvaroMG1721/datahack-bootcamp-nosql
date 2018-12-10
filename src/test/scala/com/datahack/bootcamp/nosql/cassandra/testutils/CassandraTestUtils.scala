package com.datahack.bootcamp.nosql.cassandra.testutils

import com.datahack.bootcamp.nosql.cassandra.crud.model.User
import com.datastax.driver.core.{Cluster, ResultSet, Session}

import scala.util.Try

trait CassandraTestUtils {

  def createTestConnection: Session = {
    val cluster: Try[Cluster] = Try {
      Cluster.builder()
        .addContactPoint("127.0.0.1")
        .withCredentials("cassandra", "cassandra")
        .build()
    }

    cluster.get.connect()
  }

  def createTestKeyspace(session: Session): Try[String] = Try {
    val keySpaceName = s"key${System.currentTimeMillis()}"
    session.execute(
      s"""
         |CREATE KEYSPACE $keySpaceName
         |WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}
         |""".stripMargin
    )
    keySpaceName
  }

  def createUsersTable(session: Session, keySpaceName: String): Try[Boolean] = {
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

  def createAndPopulateKeySpace(session: Session): Try[String] = {
    for {
      keySpace <- createTestKeyspace(session)
      _ <- createUsersTable(session, keySpace)
    } yield {
      keySpace
    }
  }

  def insertUsers(session: Session, keySpaceName: String, users: Seq[User]): Unit = {
    users.foreach(insertUser(session, keySpaceName, _))
  }

  def insertUser(session: Session, keySpaceName: String, user: User): ResultSet = {
    session.execute(
      s"""
         |INSERT INTO $keySpaceName.users (name, email, city, country)
         |VALUES ('${user.name}', '${user.email}', '${user.city}', '${user.country}')
      """.stripMargin)
  }

  def checkIfKeySpaceExist(session: Session, keySpaceName: String): Boolean = {
    val rs = Try(session.execute(s"USE $keySpaceName"))
    rs.map(_ => true).getOrElse(false)
  }

  def checkIfTableExist(session: Session, keySpaceName: String,  tableName: String): Boolean = {
    session.getCluster.getMetadata.getKeyspace(keySpaceName).getTable(tableName) != null
  }

  def dropKeySpace(session: Session, keySpaceName: String): Unit = {
    session.execute(s"DROP KEYSPACE IF EXISTS $keySpaceName")
  }

  def getUser(session: Session, keySpaceName: String, userName: String): Option[User] = {
    import scala.collection.JavaConverters._
    val result = session.execute(
      s"""
         |SELECT * FROM $keySpaceName.users WHERE name = '$userName'
       """.stripMargin)
    result.iterator().asScala.toSeq.headOption.map(User.fromRow)
  }
}
