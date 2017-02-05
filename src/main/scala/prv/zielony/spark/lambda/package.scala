package prv.zielony.spark

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
  * Created by Zielony on 2017-02-05.
  */
package object lambda {

  val defaultFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  implicit class StringifiableLocalDateTime(dateTime: LocalDateTime) {
    def dateTimeString: String = defaultFormatter.format(dateTime)
  }

  implicit class LocalDateTimeString(stringified: String) {
    def toLocalDateTime: LocalDateTime = LocalDateTime.parse(stringified, defaultFormatter)
  }

  implicit class LocalDateTimeLong(dateTime: LocalDateTime) {
    def toLong: Long = dateTime.dateTimeString.toLong
  }
}


