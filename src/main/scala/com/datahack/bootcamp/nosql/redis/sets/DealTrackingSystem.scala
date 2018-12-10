package com.datahack.bootcamp.nosql.redis.sets

import akka.actor.ActorSystem
import redis.RedisClient

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object DealTrackingSystem extends App {

  implicit val actorSystem: ActorSystem = akka.actor.ActorSystem()

  val redis: RedisClient = RedisClient(host = "127.0.0.1", port = 6379)

  def dealKey(dealId: Long): String = s"deal:$dealId"

  def userKey(userId: Long): String = s"user:$userId"

  def markDealAsSent(dealId: Long, userId: Long): Future[Long] = {
    redis.sadd(dealKey(dealId), userKey(userId))
  }

  def sendDealIfNotSent(dealId: Long, userId: Long): Unit = {
    redis.sismember(dealKey(dealId), userKey(userId)).map { exist =>
      if (exist) {
        println(s"Deal $dealId, was already sent to user $userId")
      } else {
        println(s"Sending $dealId to user $userId")
        // code to send the deal to the user would go here…
        markDealAsSent(dealId, userId)
      }
    }
  }

  def showUsersThatReceivedAllDeals(dealIds: Seq[Long]): Future[Unit] = {
    val keys: Seq[String] = dealIds.map(dealKey)
    redis.sinter(keys.head, keys.tail:_*).map { values =>
      values.foreach { value =>
        println(s"${value.utf8String} received all of the deals: $dealIds")
      }
    }
  }

  def  showUsersThatReceivedAtLeastOneOfTheDeals(dealIds: Seq[Long]): Future[Unit] = {
    val keys: Seq[String] = dealIds.map(dealKey)
    redis.sunion(keys.head, keys.tail: _*).map { values =>
      values.foreach { value =>
        println(s"${value.utf8String} received at least one of the deals: $dealIds")
      }
    }
  }

  def performExample: Unit = {
    markDealAsSent(1, 1)
    markDealAsSent(1, 2)
    markDealAsSent(2, 1)
    markDealAsSent(2, 3)
    sendDealIfNotSent(1, 1)
    sendDealIfNotSent(1, 2)
    sendDealIfNotSent(1, 3)
    showUsersThatReceivedAllDeals(Seq(1, 2))
    showUsersThatReceivedAtLeastOneOfTheDeals(Seq(1, 2))
  }

  performExample

  sys.addShutdownHook(actorSystem.terminate())
}
