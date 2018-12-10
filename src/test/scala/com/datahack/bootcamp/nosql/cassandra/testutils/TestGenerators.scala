package com.datahack.bootcamp.nosql.cassandra.testutils

import org.scalacheck.Gen
import com.datahack.bootcamp.nosql.cassandra.crud.model.User
import com.datahack.bootcamp.nosql.cassandra.hotel.model.{Hotel, PointOfInterest, Reservation, Room}

trait TestGenerators {

  def genUser(index: Int): Gen[User] =
    for {
      city <- Gen.oneOf("Madrid", "Salamanca", "Zamora", "Barcelona", "Sevilla")
      country <- Gen.oneOf("Spain", "Portugal", "Italy")
    } yield {
      User(
        name = s"user$index",
        email = s"user$index@mail.com",
        city = city,
        country = country
      )
    }

  def genPointName: Gen[String] = for {
    one <- Gen.oneOf("Museo", "Catedral", "Igelesia", "Palacio", "Puente", "Sala", "Parque", "Ayuntamiento", "Galeria", "Salon", "Plaza")
    two <- Gen.oneOf("Romano", "Santa Clara", "Clamores", "San Isidro", "Maldonado", "Nueva", "Mayor", "Los Mostenses", "Vieja", "San Roma", "Provincial", "Real")
  } yield s"$one $two"

  def genPointOfInterest(id: Int): Gen[PointOfInterest] = for {
    one <- Gen.oneOf("Museo", "Catedral", "Igelesia", "Palacio", "Puente", "Sala", "Parque", "Ayuntamiento", "Galeria", "Salon", "Plaza")
    two <- Gen.oneOf("Romano", "Santa Clara", "Clamores", "San Isidro", "Maldonado", "Nueva", "Mayor", "Los Mostenses", "Vieja", "San Roma", "Provincial", "Real")
    distance <- Gen.chooseNum(0.1F, 500.0F)
    address <- genCountryCity
  } yield {
    PointOfInterest(
      idHotel = id,
      name = s"$one $two",
      category = one,
      distance = distance,
      address = s"${address._2} ${address._1}",
      lat = address._3,
      long = address._4
    )
  }

  def genHotelName: Gen[String] = for {
    one <- Gen.oneOf("Hotel", "Palace", "Petit Hotel", "Casa", "Hostal", "Casona", "Parador", "Hotelito", "Maison", "Castillo", "Melia", "Vincy", "Aparta Hotel")
    two <- Gen.oneOf("Mira Sierra", "Mar de Alboran", "International", "Magenta", "Ciudad del Sol", "Las Letras", "Uria", "Garcia", "Orus", "Puerta Bonita", "Bit", "Sol", "Vacaciones", "Luxor", "Maravillas", "Los Escudos", "Paraiso", "Puerta de Hierro", "Lavapies", "Plaza Mayor", "Los Tilos", "Los Pinos", "Ritz")
  } yield s"$one $two"

  def genCountryCity: Gen[(String, String, Float, Float)] = Gen.oneOf(
    ("Spain", "Madrid", 40.4893538F, -3.6827461F),
    ("Spain", "Barcelona", 41.3818F, 2.1685F),
    ("Spain", "Salamanca", 40.9559681F, -5.6802244F),
    ("Spain", "Valencia", 39.4561165F, -0.3545661F),
    ("Spain", "Sevilla", 37.3914105F, -5.9591776F),
    ("Portugal", "Lisboa", 38.7166700F, -9.1333300F),
    ("Portugal", "Porto", 41.1496100F,  -8.6109900F),
    ("Portugal", "Cascais", 38.6979000F, -9.4214600F),
    ("Portugal", "Estoril", 38.7057100F, -9.3977300F),
    ("Italy", "Rome", 41.8919300F, 12.5113300F),
    ("Italy", "Palermo", 38.1320500F, 13.3356100F),
    ("Italy", "Milan", 45.4642700F, 9.1895100F),
    ("Italy", "Florencia", 43.7792500F, 11.2462600F),
    ("France", "Paris", 48.8534100F, 2.3488000F),
    ("France", "Cannes", 43.5513500F, 7.0127500F),
    ("France", "Bourdeaux", 44.8404400F, -0.5805000F),
    ("France", "Lyon", 45.7484600F, 4.8467100F),
    ("France", "Nice", 43.7031300F, 7.2660800F),
    ("France", "Nantes", 47.2172500F, -1.5533600F)
  )

  def genHotel(id: Int): Gen[Hotel] = for {
    name <- genHotelName
    cityCountry <- genCountryCity
    category <- Gen.chooseNum(1, 6)
  } yield Hotel(
    id = id,
    name = s"$name $id",
    city = cityCountry._2,
    country = cityCountry._1,
    category = category,
    lat = cityCountry._3,
    long = cityCountry._4
  )

  def genRoom: Gen[Room] = for {
    piso <- Gen.chooseNum(1,8)
    habitacion <- Gen.chooseNum(1, 20)
    price <- Gen.chooseNum(20.00F, 500.00F)
    info <- Gen.chooseNum(1, 3)
  } yield Room(
    idHotel = 1,
    number = s"$piso$habitacion".toInt,
    price = price,
    info = s"Number of beds: $info"
  )

  def genMonth: Gen[String] = for {
    month <- Gen.chooseNum(1, 12)
  } yield {
    if (month < 10) s"0$month"
    else month.toString
  }

  def genDay: Gen[String] = for {
    day <- Gen.chooseNum(1, 28)
  } yield {
    if (day < 10) s"0$day"
    else day.toString
  }

  def genDate: Gen[String] = for {
    month <- genMonth
    day <- genDay
  } yield s"2018-$month-$day"

  def genReservation: Gen[Reservation] = for {
    free <- Gen.oneOf(true, false)
    date <- genDate
  } yield Reservation(
    idHotel = 1,
    roomNumber = 1,
    free = free,
    date = date
  )
}
