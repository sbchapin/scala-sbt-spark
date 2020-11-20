package com.hgdata.picocli

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

import picocli.CommandLine

class InstantString extends CommandLine.ITypeConverter[Instant] {
  def convert(value: String): Instant = InstantString.convert(value)
}

object InstantString {

  private val formatter = DateTimeFormatter
    .ofPattern("yyyyMMddHHmmss")
    .withZone(ZoneId.of("GMT")) // "zoneless"

  def format(instant: Instant): String = formatter.format(instant)

  def convert(value: String): Instant = Instant.from(formatter.parse(value))
}
