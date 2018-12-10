package com.datahack.bootcamp.nosql.redis.lists

import akka.actor.ActorSystem
import redis.{RedisBlockingClient, RedisClient}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Con este ejercicio pretendemos aprender los comandos más comunes para el uso de Listas.
  * Para ello vamos a implenetar una cola genérica a la que añadiremos elementos y lo eliminaremos según los consumimos.
  * Utilizaremos el cliente RedisBlockingClient que nos va ha permitir quedarnos esperando a consumir un elemento
  * tan pronto como se produzca.
  **/

object GenericQueue extends App {

  implicit val actorSystem: ActorSystem = akka.actor.ActorSystem()

  val redis: RedisClient = ???

  // Vamos a utilizar el cliente RedisBlockingClient que permite bloquear un comando hasta que recive un resultado
  // o pasa un tiempo de timeout
  val redisBlocking = RedisBlockingClient()

  // Este método va a hacer las veces de productor, para ello primero escrive un valor en la cola "workList"
  // Espera un tiempo y publica valores en la cola "otherKeyWithWork"
  // Con esto pretendemos emular la escritura de un productor en una cola.
  def publisher(): Future[Long] = {
    // TODO: utiliza el comando lpush para añadir el valor "doSomeWork" a la lista con clave "workList".
    // http://etaty.github.io/rediscala/latest/api/redis/api/lists/Lpush.html
    ???
    Thread.sleep(2000)
    // TODO: utiliza el comando rpush para añadir los valores "doSomeWork1", "doSomeWork2"
    // a la lista con clave "otherKeyWithWork".
    // http://etaty.github.io/rediscala/latest/api/redis/api/lists/Rpush.html
    ???
  }

  // Este método va a hacer las veces de un consumidor.
  // Para que el consumidor se quede escuchando a la espera de que el productor escriba algo en la cola
  // utilizaremos el conector RedisBlockingClient
  def consumer(): Future[Unit] = Future {
    val waitWork = 3
    val sequenceFuture = for {i <- 0 to waitWork}
      yield {
        // TODO: Utiliza el método blpop para quedarte esperando a consumir un elemento de las dos listas creadas
        // por el productor
        // http://etaty.github.io/rediscala/latest/api/redis/api/blists/Blpop.html
        redisBlocking.blpop(Seq("workList", "otherKeyWithWork"), 5 seconds).foreach { result =>
          result.foreach {
            case (key, work) => println(s"list $key has work : ${work.utf8String}")
          }
        }
      }
  }

  def performExample(): Future[Long] = redis.del("workList").flatMap(_ => {
    consumer()
    publisher()
  })

  performExample()

  sys.addShutdownHook(actorSystem.terminate())

}
