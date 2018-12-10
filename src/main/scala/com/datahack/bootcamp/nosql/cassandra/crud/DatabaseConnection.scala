package com.datahack.bootcamp.nosql.cassandra.crud

import com.datastax.driver.core.{Cluster, ResultSet, Row, Session}

import scala.util.Try

object DatabaseConnection extends App {

  lazy val cluster: Try[Cluster] = ???

  def connect(): Try[Session] = ???

  def performExample: Try[Unit] = {
    connect().map { session =>
      val rs: ResultSet = session.execute("select release_version from system.local")
      val row: Row = rs.one()
      println(s"connected to Cassandra with version: ${row.getString("release_version")}")
    }
  }

  performExample

  sys.addShutdownHook(cluster.map(_.close()))

}
