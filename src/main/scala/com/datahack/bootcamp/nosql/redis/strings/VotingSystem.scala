package com.datahack.bootcamp.nosql.redis.strings

import akka.actor.ActorSystem
import redis.RedisClient

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object VotingSystem extends App {

  implicit val actorSystem: ActorSystem = akka.actor.ActorSystem()

  val redis: RedisClient = RedisClient(host = "127.0.0.1", port = 6379)

  def votesKey(id: Long): String = s"article:$id:votes"
  def headLineKey(id: Long): String = s"article:$id:headline"

  def voteUp(articleId: Long): Future[Long] =
    redis.incr(votesKey(articleId))

  def voteDown(articleId: Long): Future[Long] =
    redis.decr(votesKey(articleId))

  def showResults(articleId: Long) = {
    redis.mget( headLineKey(articleId), votesKey(articleId))
  }

  def performExample: Unit = {

    val result = for {
      _  <- redis.set("article:12345:headline", "Google Wants to Turn Your Clothes")
      _  <- redis.set("article:10001:headline", "For Millennials, the End of the TV Viewing Party")
      _  <- redis.set("article:60056:headline", "Alicia Vikander, Who Portrayed Denmark's Queen, Is Screen Royalty")

      _ <- voteUp(12345) // article:12345 has 1 vote
      _ <- voteUp(12345) // article:12345 has 2 votes
      _ <- voteUp(12345) // article:12345 has 3 votes
      _ <- voteUp(10001) // article:10001 has 1 vote
      _ <- voteUp(10001) // article:10001 has 2 votes
      _ <- voteDown(10001) // article:10001 has 1 vote
      _ <- voteUp(60056) // article:60056 has 1 vote
      result12345 <- showResults(12345)
      result10001 <- showResults(10001)
      result60056 <- showResults(60056)
    } yield {
      result12345.flatten.map(_.utf8String) ++
        result10001.flatten.map(_.utf8String) ++
        result60056.flatten.map(_.utf8String)
    }

    result.map(println)
  }

  performExample

  sys.addShutdownHook(actorSystem.terminate())
}
