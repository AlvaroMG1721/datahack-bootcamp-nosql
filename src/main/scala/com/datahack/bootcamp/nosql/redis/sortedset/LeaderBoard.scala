package com.datahack.bootcamp.nosql.redis.sortedset

import akka.util.ByteString
import redis.RedisClient

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Esta clase va ha gestionar un tablon para un juego concreto.
  * Cada tablon es un sortedset para ese juego
  *
  * @param gameName Nombre del juego para el que se crea el tablón
  * @param redis Cliente de redis para acceder a la base de datos
  */

class LeaderBoard(gameName: String, redis: RedisClient) {

  // clave del tablón
  val key = s"game:$gameName"

  // Añade un usuario a un leader board junto a su puntuación obtenida
  // TODO: utiliza el método zadd para añadir un usuario junto a su putuación en un sortedset
  // http://etaty.github.io/rediscala/latest/api/redis/api/sortedsets/Zadd.html
  def addUser(userName: String, score: Double): Future[Unit] = {
    val userAdded: Future[Long] = ???
    userAdded.map(_ => println(s"User $userName, added to the leaderboard!"))
  }

  // Elimina un usuario de un leader board
  // TODO: utiliza el método zrem para eliminar a un suario de un sortedset
  // http://etaty.github.io/rediscala/latest/api/redis/api/sortedsets/Zrem.html
  def removeUser(userName: String): Future[Unit] = {
    val userDeleted: Future[Long] = ???
    userDeleted.map(_ => println(s"User $userName, removed successfully!"))
  }

  // Imprime la puntuación y la posición de un usuario en el ranking del juego
  def getUserScoreAndRank(userName: String): Future[Unit] = {
    // TODO: utiliza el método zsocre para obtener la puntuación se un usuario
    // http://etaty.github.io/rediscala/latest/api/redis/api/sortedsets/Zscore.html
    def theScore: Future[Option[Double]] = redis.zscore(key, userName)

    // TODO: utiliza el método zrevrank para obtener la posición en el ranking del usuario
    // http://etaty.github.io/rediscala/latest/api/redis/api/sortedsets/Zrevrank.html
    def theRank: Future[Option[Long]] = redis.zrevrank(key, userName)

    for {
      score <- theScore
      rank <- theRank
    } yield {
      println(s"Details of $userName:")
      score.foreach(userScore => println(s" -- Score: $userScore"))
      rank.foreach(userRank => println(s" -- Rank: #$userRank"))
    }
  }

  // imprime el top de x usuarios (limit) de un juego
  // TODO: utiliza el comando zrevrangeWithscores para obtener los x primeros usuarios de un sorted set
  def showTopUsers(limit: Int): Future[Unit] = {
    val topUsers: Future[Seq[(ByteString, Double)]] = ???
    topUsers.map { users =>
      println(s"Top $limit users:")
      users.zipWithIndex
        .foreach(user => println(s" -- #rank: ${user._2 + 1} user: ${user._1._1.utf8String} score: ${user._1._2}"))
    }
  }

  // Devuelve los x usuarios (quantity) alrededor de un usuario concreto, junto a su puntuación y su ranking.
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

// Almacena los datos de un usuario.
case class User(rank: Long, score: Double, userName: String)
