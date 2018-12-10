package com.datahack.bootcamp.nosql.redis.strings

import akka.actor.ActorSystem
import akka.util.ByteString
import redis.RedisClient

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Con este ejercicio pretendemos aprender como utilizar los métodos para el tipo de datos String
  * Pare ello vamos ha implementar un sistema de votos para que los usuarios puedan añadir votos a un artículo
  * o por el contrario quitarle votos y de esta forma poder definir su popularidad.
  **/


object VotingSystem extends App {

  implicit val actorSystem: ActorSystem = akka.actor.ActorSystem()

  val redis: RedisClient = ???

  // Este método construye la clave para los votos de un artículo
  def votesKey(id: Long): String = s"article:$id:votes"
  // Este método construye la clave para el titular de un artículo
  def headLineKey(id: Long): String = s"article:$id:headline"

  // TODO: utiliza incr para añadir un voto al artículo con id articleId
  // http://etaty.github.io/rediscala/latest/api/redis/api/strings/Incr.html
  def voteUp(articleId: Long): Future[Long] = ???

  // TODO: utilza decr para eliminar un voto al artículo con id articleId
  // http://etaty.github.io/rediscala/latest/api/index.html#redis.api.strings.Decr
  def voteDown(articleId: Long): Future[Long] = ???

  // TODO: utiliza mget para obtener los votos de un artículo con id articleID
  // http://etaty.github.io/rediscala/latest/api/index.html#redis.api.strings.Mget
  def showResults(articleId: Long): Future[Seq[Option[ByteString]]] = ???

  def performExample: Unit = {

    val result = for {
      _  <- redis.set(headLineKey(12345), "Google Wants to Turn Your Clothes")
      _  <- redis.set(headLineKey(10001), "For Millennials, the End of the TV Viewing Party")
      _  <- redis.set(headLineKey(60056), "Alicia Vikander, Who Portrayed Denmark's Queen, Is Screen Royalty")

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
