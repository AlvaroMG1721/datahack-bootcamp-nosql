package com.datahack.bootcamp.nosql.redis.sets

import akka.actor.ActorSystem
import akka.util.ByteString
import redis.RedisClient

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Con este ejercicio pretendemos aprender a utilizar los métodos más comunes del tipo de datos set
  * Para ello vamos a suponer que una empresa de estilo Groupon manda todos los días un email con una serie de ofertas
  * (cupones) a los usuarios interesados.
  * Queremos:
  *    - Saber cuando una oferta se ha enviado a un usuarios
  *    - Comprobar cuando un usuario a recivido un grupo de cupones
  *    - Obtener métricas de las ofertas enviadas
  **/

object DealTrackingSystem extends App {

  implicit val actorSystem: ActorSystem = akka.actor.ActorSystem()

  val redis: RedisClient = RedisClient(host = "127.0.0.1", port = 6379)

  // Este método crea la clave para guardra una oferta
  def dealKey(dealId: Long): String = s"deal:$dealId"

  // Este método crea la clave para guardar un usuario
  def userKey(userId: Long): String = s"user:$userId"

  // Este método marca una oferta con un id dealId a un usuario con id userId
  // TODO: utiliza el método sadd para añadir al set de la oferta dealId el id del usuario al que se ha mandado
  // la oferta con id userId
  // http://etaty.github.io/rediscala/latest/api/redis/api/sets/Sadd.html
  def markDealAsSent(dealId: Long, userId: Long): Future[Long] = ???

  // Este método comprueba si una oferta ha sido enviada a un usuario. Si no ha sido enviada, la envía y la marca
  // como envida. Para ello utiliza la función markDealAsSent
  // TODO: utiliza el método sismember para saber si el set de la oferta contiene el valor de userId al que se
  // pretende enviar la oferta.
  // http://etaty.github.io/rediscala/latest/api/redis/api/sets/Sismember.html
  def sendDealIfNotSent(dealId: Long, userId: Long): Unit = {
    val maybeExist: Future[Boolean] = ???
    maybeExist.map { exist =>
      if (exist) {
        println(s"Deal $dealId, was already sent to user $userId")
      } else {
        println(s"Sending $dealId to user $userId")
        // Código para enviar el cupón aquí.
        markDealAsSent(dealId, userId)
      }
    }
  }

  // Este método informa de todos los usuarios a los que les ha llegado una conjunto de ofertas
  // TODO: utiliza el método sinter para obtener todos los usuarios que están contenidos en los dos sets
  // de las ofertas que quermos consultar (intersección de los conjuntos de las ofertas)
  // http://etaty.github.io/rediscala/latest/api/redis/api/sets/Sinter.html
  def showUsersThatReceivedAllDeals(dealIds: Seq[Long]): Future[Unit] = {
    val keys: Seq[String] = dealIds.map(dealKey)
    val userIds: Future[Seq[ByteString]] = ???
    userIds.map { values =>
      values.foreach { value =>
        println(s"${value.utf8String} received all of the deals: $dealIds")
      }
    }
  }

  // Este método informa de todos los usuarios a los que les ha llegado al menos una oferta de las indicadas.
  // TODO: utiliza el método sunion para obtener todos los usuarios que están contenidos en al menos uno de los sets
  // de las ofertas que quermos consultar (unión de los conjuntos de las ofertas)
  // http://etaty.github.io/rediscala/latest/api/redis/api/sets/Sunion.html
  def  showUsersThatReceivedAtLeastOneOfTheDeals(dealIds: Seq[Long]): Future[Unit] = {
    val keys: Seq[String] = dealIds.map(dealKey)
    val userIds: Future[Seq[ByteString]] = ???
    userIds.map { values =>
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
