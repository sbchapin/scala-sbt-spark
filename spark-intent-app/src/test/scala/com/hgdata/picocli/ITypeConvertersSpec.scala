package com.hgdata.picocli

import java.time.Instant
import java.time.format.DateTimeParseException

import com.hgdata.picocli.ITypeConverters.{Date, HudiInstant, ZonedTimestamp}
import org.scalatest.FunSpec

class ITypeConvertersSpec extends FunSpec {

  describe("A HudiInstant converter") {

    it("should format to yyyyMMddHHmmss") {
      assert( HudiInstant.format(Instant.EPOCH) == "19700101000000")
    }

    it("should parse yyyyMMddHHmmss") {
      assert( HudiInstant.convert("19700101000000") == Instant.EPOCH)
    }

    it("should not parse timeless formats") {
      assertThrows[DateTimeParseException] { HudiInstant.convert("19700101") }
    }
  }

  describe("A ZonedTimestamp converter") {

    it("should format to yyyy-MM-ddTHH:mm:ssZ") {
      assert( ZonedTimestamp.format(Instant.EPOCH) == "1970-01-01T00:00:00Z")
    }

    it("should parse yyyy-MM-ddTHH:mm:ssZ") {
      assert( ZonedTimestamp.convert("1970-01-01T00:00:00Z") == Instant.EPOCH)
    }

    it("should parse yyyy-MM-ddTHH:mm:ss+TT:TT") {
      assert( ZonedTimestamp.convert("1970-01-01T00:00:00+00:00") == Instant.EPOCH)
    }

    it("should not parse timeless formats") {
      assertThrows[DateTimeParseException] { ZonedTimestamp.convert("1970-01-01") }
    }
  }


  describe("A Date converter") {

    it("should format to yyyy-MM-dd") {
      assert(Date.format(Instant.EPOCH) == "1970-01-01")
    }

    it("should parse yyyy-MM-dd") {
      assert(Date.convert("1970-01-01") == Instant.EPOCH)
    }

    it("should not parse time formats") {
      assertThrows[DateTimeParseException] { ZonedTimestamp.convert("1970-01-01 00:00:00") }
    }
  }
}
