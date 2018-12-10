package com.datahack.bootcamp.nosql.cassandra.crud

import com.datahack.bootcamp.nosql.cassandra.crud.model.User
import com.datahack.bootcamp.nosql.cassandra.testutils.{CassandraTestUtils, TestGenerators}
import com.datastax.driver.core.Session
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

class CrudSpec
  extends WordSpec
    with Matchers
    with BeforeAndAfterAll
    with CassandraTestUtils
    with TestGenerators {

  lazy val session: Session = createTestConnection

  lazy val crud: Crud = new Crud(session)

  val keySpaceName = "testspace"

  var testKeySpaceName: String = ""
  var keySpacePopulated: String = ""

  val users: Seq[User] = (1 to 5).map(i => genUser(i).sample.get)

  override protected def beforeAll(): Unit = {
    testKeySpaceName = createTestKeyspace(session).get
    keySpacePopulated = createAndPopulateKeySpace(session).get
    insertUsers(session, keySpacePopulated, users)
  }


  "Crud class" should {

    "create a new key space" in {
      crud.createKeySpace(keySpaceName, "SimpleStrategy", 1)
      checkIfKeySpaceExist(session, keySpaceName) shouldBe true
    }

    "create users table" in {
      crud.createTableUsers(testKeySpaceName)
      checkIfTableExist(session, testKeySpaceName, "users") shouldBe true
    }

    "get all users stored into keyspace" in {
      crud.getAllUsers(keySpacePopulated) should contain allElementsOf users
    }

    "get an stored user" in {
      crud.getUserByName(keySpacePopulated, users.head.name) shouldBe Some(users.head)
    }

    "update an stored user" in {
      val userToUpdate = users.last.copy(email = "thenew@mail.com")
      crud.updateUser(keySpacePopulated, userToUpdate)
      getUser(session, keySpacePopulated, userToUpdate.name) shouldBe Some(userToUpdate)
    }

    "insert data into users table" in {
      val user: User = genUser(users.length + 1).sample.get
      crud.insertIntoUsers(keySpacePopulated, user)
      getUser(session, keySpacePopulated, user.name) shouldBe Some(user)
    }

    "delete an stored user" in {
      crud.deleteUser(keySpacePopulated, users.last.name)
      getUser(session, keySpacePopulated, users.last.name) shouldBe None
    }

  }

  override protected def afterAll(): Unit = {
    dropKeySpace(session, keySpaceName)
    dropKeySpace(session, testKeySpaceName)
    dropKeySpace(session, keySpacePopulated)
  }
}
