package com.hgdata.picocli

import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField
import java.time.{Instant, ZoneId}

import picocli.CommandLine

import scala.util.Try

object ITypeConverters {

  class LenientInstant extends CommandLine.ITypeConverter[Instant] {

    private val strategies = Seq(
      HudiInstant.convert(_),
      ZonedTimestamp.convert(_),
      Date.convert(_)
    )

    def convert(value: String): Instant = {
      for (strategy <- strategies) {
        val attempt = Try(strategy(value))
        if (attempt.isSuccess) return attempt.get
      }
      throw new CommandLine.TypeConversionException(s"Could not convert ${value} to a value of type 'instant'.")
    }
  }

  object HudiInstant {

    private val formatter = DateTimeFormatter
      .ofPattern("yyyyMMddHHmmss")
      .withZone(ZoneId.of("GMT")) // "zoneless"

    def format(instant: Instant): String = formatter.format(instant)

    def convert(value: String): Instant = Instant.from(formatter.parse(value))
  }

  object ZonedTimestamp {

    private val formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneId.of("GMT")) // "zoneless"

    def format(instant: Instant): String = formatter.format(instant)

    def convert(value: String): Instant = Instant.from(formatter.parse(value))
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
