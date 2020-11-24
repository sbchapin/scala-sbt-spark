package com.hgdata.spark

import com.hgdata.spark.io.Reader
import com.hgdata.spark.testutil.{IntentFixtures, SparkHelpers}
import org.apache.spark.sql.SaveMode
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class MainE2ESpec extends FunSpec with BeforeAndAfterAll with SparkHelpers {

  object AUD {
    val command = "alternate-url-deltify"
    val inputPath = s"./tmp/in/$command/2020-01-01" // timestamp necessary to create "run_id"
    val outputPath = s"./tmp/out/$command"
  }

  object IP {
    val command = "intent-prep"
    val inputPath = s"./tmp/in/$command"
    val outputPath = s"./tmp/out/$command"
  }

  object IN {
    val command = "intent-update"
    val inputAlternateUrlsPath: String = AUD.outputPath
    val inputIntentPrepPath: String = IP.outputPath
    val outputPath = s"./tmp/out/$command"
  }

  override protected def beforeAll(): Unit = {
    // Set up I/O needed for tests:
    withTestSpark { spark =>
      import spark.implicits._

      // For alternate-url-deltify:
      val alts = Seq(
        ("subdomain.hginsights.com", "hginsights.com", "distinct_child_entity"),
        ("hginsights.info",          "hginsights.com", "alias"),
        ("hginsights.com",           "hginsights.com", "alias")
      ).toDF("alternate_url", "url", "alternate_url_type")
      alts.write.mode(SaveMode.Overwrite).parquet(AUD.inputPath)

      // For intent prep:
      val intentRaw = Seq(
        IntentFixtures.header,
        IntentFixtures.hgRow
      ).toDF
      intentRaw.write.mode(SaveMode.Overwrite).text(IP.inputPath)
    }
  }

  describe("The Main entrypoint") {

    describe(s"running the ${AUD.command} command") {
      it ("should not explode catastrophically") {
        withTestSparkSysProps {
          Main.main(Array(
            AUD.command,
            "-i", AUD.inputPath,
            "-o", AUD.outputPath
          ))
          withTestSpark { implicit spark =>
            new Reader.HudiSnapshot(AUD.outputPath, 1).read.show()
          }
        }
      }
    }

    describe(s"running the ${IP.command} command") {
      it ("should not explode catastrophically") {
        withTestSparkSysProps {
          Main.main(Array(
            IP.command,
            "-i", IP.inputPath,
            "-o", IP.outputPath
          ))
          withTestSpark { implicit spark =>
            new Reader.HudiSnapshot(IP.outputPath, 1).read.show()
          }
        }
      }
    }

    describe(s"running the ${IN.command} command") {
      it ("should not explode catastrophically") {
        withTestSparkSysProps {
          Main.main(Array(
            IN.command,
            "--input-alternate-urls-path", IN.inputAlternateUrlsPath,
            "--input-prepped-intent-path", IN.inputIntentPrepPath,
            "--input-prepped-intent-since", "19991231235959", // Y2K
            "-o", IN.outputPath
          ))
          withTestSpark { implicit spark =>
            new Reader.HudiSnapshot(IN.outputPath, 1).read.show()
          }
        }
      }
    }
  }
}
