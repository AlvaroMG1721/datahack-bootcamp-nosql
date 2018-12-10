package com.datahack.bootcamp.nosql.redis.bitmap

import akka.actor.ActorSystem
import akka.util.ByteString
import redis.RedisClient

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Con este ejercicio se pretende aprender los comandos mas comunes para los bitmaps
  * Vamos a implementar un sistema para contabilizar las visitas de una web
  * Para cada día vamos a utilizar una entrada nuava (una nueva clave)
  * El valor asociado a esa clave es un bitmap, de tal forma que cada usuairo representa una posición del bitmap
  * y su valor 0 o 1 indica si se ha visitado (1) o no (0)
  **/

object WebAnalytics extends App {

  implicit val actorSystem: ActorSystem = akka.actor.ActorSystem()

  val redis: RedisClient = RedisClient(host = "127.0.0.1", port = 6379)

  // Genera la clave para cada día
  def visitsKey(date: String) = s"visits:daily:$date"

  // Almacena una visita para un usuario en un día concreto
  // TODO: utiliza el método setbit para poner a 1 la posición del userId en el bitmap
  // http://etaty.github.io/rediscala/latest/api/redis/api/strings/Setbit.html
  def storeDailyVisit(date: String, userId: Int): Future[Boolean] = ???

  // Cuenta todas las visitas para un día concreto
  // TODO: utilza el método bitcount para obtener el número de bits del bitmap
  // http://etaty.github.io/rediscala/latest/api/redis/api/strings/Bitcount.html
  def countVisits(date: String): Future[Long] = ???

  // obriene los ids de los usuarios que han visitado la página en un día concreto
  // TODO: utiliza el método get para obtener el bitmap asociado a una fecha
  // http://etaty.github.io/rediscala/latest/api/redis/api/strings/Get.html
  def showUserIdsFromVisit(date: String): Future[Seq[Option[Int]]] = {
    val vistis: Future[Option[ByteString]] = ???
    vistis.map { someDateBiteMap =>
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
