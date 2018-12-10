package com.datahack.bootcamp.nosql.redis.hashes

import akka.actor.ActorSystem
import _root_.redis.RedisClient

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object VotingSystem extends App {

  implicit val actorSystem: ActorSystem = akka.actor.ActorSystem()

  val redis: RedisClient = RedisClient(host = "127.0.0.1", port = 6379)

  def linkKey(linkId: Long) = s"link:$linkId"

  def saveLink(id: Long, author: String, title: String, link: String): Future[Boolean] = {
    val values = Map[String, String] (
      "author" -> author,
      "title" -> title,
      "link" -> link,
      "score" -> "0"
    )
    redis.hmset(linkKey(id), values)
  }

  def voteUp(linkId: Long): Future[Long] =
    redis.hincrby(linkKey(linkId), "score", 1)

  def voteDown(linkId: Long): Future[Long] =
    redis.hincrby(linkKey(linkId), "score", -1)

  def showResults(linkId: Long): Future[Unit] = {
    println(s"Key: ${linkKey(linkId)}")
    redis.hgetall(linkKey(linkId)).map { values =>
      val keys = values.keys
      keys.foreach { key =>
        values.get(key).foreach(value => println(s" -- $key: ${value.utf8String}"))
      }
    }
  }

  def performExample: Unit = {
    saveLink(123, "dayvson", "Maxwell Dayvson's Github page", "https:// github.com/dayvson")
    voteUp(123)
    voteUp(123)
    saveLink(456, "hltbra", "Hugo Tavares's Github page", "https://github. com/hltbra")
    voteUp(456)
    voteUp(456)
    voteDown(456)
    Await.result(showResults(123), 5 seconds)
    Await.result(showResults(456), 5 seconds)
  }

  performExample

  sys.addShutdownHook(actorSystem.terminate())
}
