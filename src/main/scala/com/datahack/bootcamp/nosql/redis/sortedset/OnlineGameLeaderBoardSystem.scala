package com.datahack.bootcamp.nosql.redis.sortedset

import akka.actor.ActorSystem
import redis.RedisClient

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Con este ejercicio se pretende aprender a usar los métodos más comunes para el tipo de datos sortedset
  * Para ello vamos a implementar un tablón de puntuaciones de jugadores
  * Nos va a permitir añadir las puntuaciones obtenidas para un jugador y su posición en el ranking de jugadores.
  **/

object OnlineGameLeaderBoardSystem extends App {

  implicit val actorSystem: ActorSystem = akka.actor.ActorSystem()

  val redis: RedisClient = RedisClient(host = "127.0.0.1", port = 6379)

  // Para implementar la lógica vamos a utilizar la clase LeaderBorad
  val leaderBoard = new LeaderBoard("game-score", redis)
  leaderBoard.addUser("Arthur", 70)
  leaderBoard.addUser("KC", 20)
  leaderBoard.addUser("Maxwell", 10)
  leaderBoard.addUser("Patrik", 30)
  leaderBoard.addUser("Ana", 60)
  leaderBoard.addUser("Felipe", 40)
  leaderBoard.addUser("Renata", 50)
  leaderBoard.addUser("Hugo", 80)
  leaderBoard.removeUser("Arthur")
  leaderBoard.getUserScoreAndRank("Maxwell")
  leaderBoard.showTopUsers(3)

  leaderBoard.getUsersAroundUser("Felipe", 5).map { users =>
    println("Users around Felipe")
    users.foreach { user =>
      println(s"#${user.rank} User: ${user.userName} score: ${user.score}")
    }
  }

  sys.addShutdownHook(actorSystem.terminate())
}
