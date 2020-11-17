package com.hgdata.spark

import com.hgdata.spark.io.Reader
import com.hgdata.spark.testutil.SparkHelpers
import org.apache.spark.sql.SaveMode
import org.scalatest.FunSpec

class MainE2ESpec extends FunSpec with SparkHelpers {

  describe("The Main entrypoint") {

    val commandAUD = "alternate-url-deltify"

    // Set up I/O needed for tests:
    val inputPath = "./tmp/in/2020-01-01/" // timestamp necessary to create "run_id"
    val outputPath = "./tmp/out"
    withTestSpark { spark =>
      import spark.implicits._
      val alts = Seq(
        ("aye.com", "a.com", "distinct_child_entity"),
        ("a.com",   "a.com", "alias"),
        ("ayy.com", "a.com", "alias")
      ).toDF("alternate_url", "url", "alternate_url_type")
      alts.write.mode(SaveMode.Overwrite).parquet(inputPath)
    }

    describe(s"running the ${commandAUD} command") {

      it ("should not explode catastrophically") {
        // Run main:
        withTestSparkSysProps {
          Main.main(Array(
            commandAUD,
            "-i", inputPath,
            "-o", outputPath
          ))
          withTestSpark { implicit spark =>
            new Reader.HudiSnapshot(outputPath, 1).read.show()
          }
        }
      }
    }
  }
}
