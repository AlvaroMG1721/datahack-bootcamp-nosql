package com.datahack.bootcamp.nosql.redis.bitmap

import akka.actor.ActorSystem
import redis.RedisClient

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object WebAnalytics extends App {

  implicit val actorSystem: ActorSystem = akka.actor.ActorSystem()

  val redis: RedisClient = RedisClient(host = "127.0.0.1", port = 6379)

  def visitsKey(date: String) = s"visits:daily:$date"

  def storeDailyVisit(date: String, userId: Int): Future[Boolean] = {
    redis.setbit(visitsKey(date), userId, true)
  }

  def countVisits(date: String): Future[Long] = {
    redis.bitcount(visitsKey(date))
  }

  def showUserIdsFromVisit(date: String): Future[Seq[Option[Int]]] = {
    redis.get(visitsKey(date)).map { someDateBiteMap =>
      someDateBiteMap.map { dateBitMap =>
        dateBitMap.zipWithIndex.map { byteWithIndex =>
          val byte = byteWithIndex._1
          val byteIndex = byteWithIndex._2
          (7 to 0 by -1).map { bitIndex =>
            val visited = byte >> bitIndex & 1
            if (visited == 1) {
              Some(byteIndex * 8 + (7 - bitIndex))
            } else {
              None
            }
          }
        }.reduce(_ ++ _)
      }.getOrElse(Seq.empty[Option[Int]])
    }
  }

  def performExample = {
    storeDailyVisit("2015-01-01", 1)
    storeDailyVisit("2015-01-01", 2)
    storeDailyVisit("2015-01-01", 10)
    storeDailyVisit("2015-01-01", 55)
    countVisits("2015-01-01").map(visits => println(s"2015-01-01 had $visits visits"))
    showUserIdsFromVisit("2015-01-01").map { users =>
      users.flatten.foreach(userId => println(s"User $userId visited on 2015-01-01"))
    }
  }

  performExample

  sys.addShutdownHook(actorSystem.terminate())
}
