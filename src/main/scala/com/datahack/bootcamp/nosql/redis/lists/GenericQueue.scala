package com.datahack.bootcamp.nosql.redis.lists

import akka.actor.ActorSystem
import redis.{RedisBlockingClient, RedisClient}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object GenericQueue extends App {

  implicit val actorSystem: ActorSystem = akka.actor.ActorSystem()

  val redis: RedisClient = RedisClient(host = "127.0.0.1", port = 6379)

  val redisBlocking = RedisBlockingClient()

  def publisher() = {
    redis.lpush("workList", "doSomeWork")
    Thread.sleep(2000)
    redis.rpush("otherKeyWithWork", "doSomeWork1", "doSomeWork2")
  }

  def consumer() = Future {
    val waitWork = 3
    val sequenceFuture = for {i <- 0 to waitWork}
      yield {
        redisBlocking.blpop(Seq("workList", "otherKeyWithWork"), 5 seconds).foreach { result =>
          result.foreach {
            case (key, work) => println(s"list $key has work : ${work.utf8String}")
          }
        }
      }

    sequenceFuture
  }

  def performExample(): Future[Long] = redis.del("workList").flatMap(_ => {
    consumer()
    publisher()
  })

  performExample()

  sys.addShutdownHook(actorSystem.terminate())

}
