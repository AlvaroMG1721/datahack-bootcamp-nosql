package com.datahack.bootcamp.nosql.redis.hashes

import akka.actor.ActorSystem
import _root_.redis.RedisClient
import akka.util.ByteString

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Con ese ejercicio pretendemos aprender como utilizar el tipo de datos Hash y sus comandos más comunes.
  * Para ello vamos a volver a utilizar el caso de uso del voting sistem, pero en esta ocasión utilizaremos
  * hashes en vez de strings.
  **/

object VotingSystem extends App {

  implicit val actorSystem: ActorSystem = akka.actor.ActorSystem()

  val redis: RedisClient = ???

  // Este método crea la clave para un link concreto a partir de su id.
  def linkKey(linkId: Long) = s"link:$linkId"

  // Este método guarda en un hash los datos de un artículo
  def saveLink(id: Long, author: String, title: String, link: String): Future[Boolean] = {
    val values = Map[String, String] (
      "author" -> author,
      "title" -> title,
      "link" -> link,
      "score" -> "0"
    )
    // TODO: utiliza hmset para almacenar su información (values) en el hash
    // http://etaty.github.io/rediscala/latest/api/redis/api/hashes/Hmset.html
    ???
  }

  // Este método incrementa el voto para el articulo con id linkId
  // TODO: utiliza hincrby para aumentar en 1 el valor del atributo score del artículo
  // http://etaty.github.io/rediscala/latest/api/redis/api/hashes/Hincrby.html
  def voteUp(linkId: Long): Future[Long] = ???

  // Este método decrementa el voto para el articulo con id linkId
  // TODO: utiliza hincrby para decrementar en 1 el valor del atributo score del artículo
  // http://etaty.github.io/rediscala/latest/api/redis/api/hashes/Hincrby.html
  def voteDown(linkId: Long): Future[Long] = ???

  // Este método recoge los valores de los atributos almacenados para el artículo con id linkId
  // TODO: utiliza el método hgetall para obtener todos los valores asociados a la clave del artículo con id linkId
  // http://etaty.github.io/rediscala/latest/api/redis/api/hashes/Hgetall.html
  def showResults(linkId: Long): Future[Unit] = {
    println(s"Key: ${linkKey(linkId)}")
    val attributes: Future[Map[String, ByteString]] = ???
    attributes.map { values =>
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
