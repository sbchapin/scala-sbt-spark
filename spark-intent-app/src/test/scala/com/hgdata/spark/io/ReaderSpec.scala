package com.hgdata.spark.io

import com.hgdata.generated.BuildInfo
import com.hgdata.spark.testutil.{IOHelpers, IntentFixtures, SparkHelpers}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.scalatest.FunSpec

class ReaderSpec extends FunSpec with SparkHelpers with IOHelpers {

  describe("A RawIntent Reader") {
    it("should use local resource file to create valid dataframe") {
      withTestSpark { implicit spark: SparkSession =>
        val reader: Reader = new Reader.MetroLookup
        val metros = reader.read
        assert(
          metros.schema.names.contains("city_1") &&
            metros.schema.names.contains("city_2") &&
            metros.schema.names.contains("city_3") &&
            metros.schema.names.contains("country") &&
            metros.schema.names.contains("country_code") &&
            metros.schema.names.contains("metro_area") &&
            metros.schema.names.contains("state") &&
            metros.schema.names.contains("metro_version")
        )
        val abilene = metros.filter(metros("metro_area") === "abilene, texas area").head
        assert(
          abilene(0) == "Abilene" &&
            abilene(1) == null &&
            abilene(2) == null &&
            abilene(3) == "United States" &&
            abilene(4) == "US" &&
            abilene(5) == "abilene, texas area" &&
            abilene(6) == "TX" &&
            abilene(7) == BuildInfo.version
        )
      }
    }
  }
  describe("A RawIntent Reader") {

    it("should ignore header, parsing row data with quoted values, quote-escaping quotes, and commas") {
      withTestSpark { implicit spark: SparkSession =>
        val reader: Reader = new Reader.RawIntent(createTmpFile(
          IntentFixtures.of(IntentFixtures.hgRow)
        ))
        val firstRow = reader.read.take(1).head
        assert(
          firstRow(0) == """HG "Insights", LLC""" &&
            firstRow(1) == "hginsights.com" &&
            firstRow(2) == null &&
            firstRow(3) == null &&
            firstRow(4) == "programming languages" &&
            firstRow(5) == "scala" &&
            firstRow(6) == 80 &&
            firstRow(7) == null &&
            firstRow(8) == 0 &&
            firstRow(9) == "united states" &&
            firstRow(10) == "2020-10-17" &&
            firstRow(11) == null )
      }
    }
  }
}
