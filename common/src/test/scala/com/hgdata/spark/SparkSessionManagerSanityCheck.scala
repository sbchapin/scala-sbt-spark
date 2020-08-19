package com.hgdata.spark

import java.util

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSpec

class SparkSessionManagerSanityCheck extends FunSpec {

  val sparkConf = Map(
    "spark.master" -> "local[*]"
  )

  describe("A SparkSessionManager") {

    it("should not create spark session unless asked to") {
      SparkSessionManager(sparkConf)
      assert(SparkSession.getActiveSession.isEmpty)
    }

    it("should create a spark session when asked to") {
      SparkSessionManager(sparkConf).withSpark { providedSession =>
        val sparkSession = SparkSession.getActiveSession
        assert(sparkSession.nonEmpty && sparkSession.get == providedSession)
      }
    }
  }
}
