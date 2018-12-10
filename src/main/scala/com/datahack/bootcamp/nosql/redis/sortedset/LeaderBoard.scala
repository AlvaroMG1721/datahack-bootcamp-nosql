package com.datahack.bootcamp.nosql.redis.sortedset

import redis.RedisClient

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class LeaderBoard(gameName: String, redis: RedisClient) {

  val key = s"game:$gameName"

  def addUser(userName: String, score: Double): Future[Unit] = {
    redis.zadd(key, (score, userName)).map(_ => println(s"User $userName, added to the leaderboard!"))
  }

  def removeUser(userName: String): Unit = {
    redis.zrem(key, userName).map(_ => println(s"User $userName, removed successfully!"))
  }

  def getUserScoreAndRank(userName: String): Future[Unit] = {
    for {
      score <- redis.zscore(key, userName)
      rank <- redis.zrevrank(key, userName)
    } yield {
      println(s"Details of $userName:")
      score.foreach(userScore => println(s" -- Score: $userScore"))
      rank.foreach(userRank => println(s" -- Rank: #$userRank"))
    }
  }

  def showTopUsers(limit: Int): Future[Unit] = {
    redis.zrevrangeWithscores(key, 0, limit - 1).map { users =>
      println(s"Top $limit users:")
      users.zipWithIndex
        .foreach(user => println(s" -- #rank: ${user._2 + 1} user: ${user._1._1.utf8String} score: ${user._1._2}"))
    }
  }

  def getUsersAroundUser(userName: String, quantity: Int): Future[Seq[User]] = {

    def startOffset(rank: Long): Long = Math.floor(rank - (quantity / 2) + 1).toLong

    def endOffset(rank: Long): Long = startOffset(rank) + quantity + 1

    for {
      rank <- redis.zrevrank(key, userName)
      range <- redis.zrevrangeWithscores(key, rank.map(startOffset).getOrElse(0), rank.map(endOffset).getOrElse(0))
    } yield {
      range.zipWithIndex.map { r =>
        User(
          rank =  rank.get + r._2,
          score = r._1._2,
          userName = r._1._1.utf8String
        )
      }
    }
  }
}

case class User(rank: Long, score: Double, userName: String)
