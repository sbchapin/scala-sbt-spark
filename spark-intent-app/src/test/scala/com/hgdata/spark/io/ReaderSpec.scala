package com.hgdata.spark.io

import com.hgdata.spark.testutil.{IOHelpers, SparkHelpers}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSpec

class ReaderSpec extends FunSpec with SparkHelpers with IOHelpers {

  describe("A RawIntent Reader") {

    it("should ignore header, parsing row data with quoted values, quote-escaping quotes, and commas") {
      withTestSpark { implicit spark: SparkSession =>
        val reader: Reader = new Reader.RawIntent(createTmpFile(
          """Company Name,Domain,Size,Industry,Category,Topic,Composite Score,Metro Area,Metro Composite Score,Domain Origin,Date Stamp,HVC Level1 Trend
            |"HG ""Insights"", LLC",hginsights.com,,,programming languages,scala,80,,0,united states,2020-10-17,
            |""".stripMargin
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
