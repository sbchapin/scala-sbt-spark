package com.hgdata.picocli

import java.time.{Instant, ZoneId}
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField

import picocli.CommandLine

object ITypeConverters {

  class HudiInstant extends CommandLine.ITypeConverter[Instant] {
    def convert(value: String): Instant = HudiInstant.convert(value)
  }

  object HudiInstant {

    private val formatter = DateTimeFormatter
      .ofPattern("yyyyMMddHHmmss")
      .withZone(ZoneId.of("GMT")) // "zoneless"

    def format(instant: Instant): String = formatter.format(instant)

    def convert(value: String): Instant = Instant.from(formatter.parse(value))
  }

  class ZonedTimestamp extends CommandLine.ITypeConverter[Instant] {
    def convert(value: String): Instant = ZonedTimestamp.convert(value)
  }

  object ZonedTimestamp {

    private val formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneId.of("GMT")) // "zoneless"

    def format(instant: Instant): String = formatter.format(instant)

    def convert(value: String): Instant = Instant.from(formatter.parse(value))
  }


  class Date extends CommandLine.ITypeConverter[Instant] {
    def convert(value: String): Instant = Date.convert(value)
  }

  object Date {

    private val formatter = new DateTimeFormatterBuilder()
        .appendPattern("yyyy-MM-dd")
        .parseDefaulting(ChronoField.NANO_OF_DAY, 0)
        .toFormatter()
        .withZone(ZoneId.of("GMT")) // "zoneless"

    def format(instant: Instant): String = formatter.format(instant)

    def convert(value: String): Instant = Instant.from(formatter.parse(value))
  }

}
