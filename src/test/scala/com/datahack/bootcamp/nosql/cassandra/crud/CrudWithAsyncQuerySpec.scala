package com.datahack.bootcamp.nosql.cassandra.crud

import com.datahack.bootcamp.nosql.cassandra.crud.model.User
import com.datahack.bootcamp.nosql.cassandra.testutils.{CassandraTestUtils, TestGenerators}
import com.datastax.driver.core.Session
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.Await
import scala.concurrent.duration._

class CrudWithAsyncQuerySpec
  extends WordSpec
    with Matchers
    with BeforeAndAfterAll
    with CassandraTestUtils
    with TestGenerators {

  lazy val session: Session = createTestConnection

  lazy val crud: CrudWithAsyncQuery = new CrudWithAsyncQuery(session)

  var keySpacePopulated: String = ""

  val users: Seq[User] = (1 to 5).map(i => genUser(i).sample.get)

  override protected def beforeAll(): Unit = {
    keySpacePopulated = createAndPopulateKeySpace(session).get
    insertUsers(session, keySpacePopulated, users)
  }


  "Crud class" should {

    "get all users stored into keyspace" in {
      Await.result(crud.getAllUsers(keySpacePopulated), 5 seconds) should contain allElementsOf users
    }

    "get an stored user" in {
      Await.result(crud.getUserByName(keySpacePopulated, users.head.name), 5 seconds) shouldBe Some(users.head)
    }

    "update an stored user" in {
      val userToUpdate = users.last.copy(email = "thenew@mail.com")
      Await.result(crud.updateUser(keySpacePopulated, userToUpdate), 5 seconds)
      getUser(session, keySpacePopulated, userToUpdate.name) shouldBe Some(userToUpdate)
    }

    "insert data into users table" in {
      val user = genUser(users.length + 1).sample.get
      Await.result(crud.insertIntoUsers(keySpacePopulated, user), 5 seconds)
      getUser(session, keySpacePopulated, user.name) shouldBe Some(user)
    }

    "delete an stored user" in {
      Await.result(crud.deleteUser(keySpacePopulated, users.last.name), 5 seconds)
      getUser(session, keySpacePopulated, users.last.name) shouldBe None
    }

  }

  override protected def afterAll(): Unit = {
    dropKeySpace(session, keySpacePopulated)
  }
}
