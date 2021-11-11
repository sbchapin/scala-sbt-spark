package com.hgdata.spark

import com.hgdata.spark.io.Reader
import com.hgdata.spark.testutil.{IntentFixtures, SparkHelpers}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class MainE2ESpec extends FunSpec with BeforeAndAfterAll with SparkHelpers {

  object AltUrlDeltify {
    val command = "alternate-url-deltify"
    val inputPath = s"./tmp/in/$command/2020-01-01" // timestamp necessary to create "run_id"
    val outputPath = s"./tmp/out/$command"
  }

  object MetroLookupDeltify {
    val command = "metro-lookup-deltify"
    val outputPath = s"./tmp/out/$command"
  }

  object IntentPrep {
    val command = "intent-prep"
    val inputPath = s"./tmp/in/$command"
    val outputPath = s"./tmp/out/$command"
  }

  object IntentUpdate {
    val command = "intent-update"
    val inputAlternateUrlsPath: String = AltUrlDeltify.outputPath
    val inputMetroLookupPath: String = MetroLookupDeltify.outputPath
    val inputIntentPrepPath: String = IntentPrep.outputPath
    val outputPath = s"./tmp/out/$command"
  }

  override protected def beforeAll(): Unit = {
    // Set up I/O needed for tests:
    withTestSpark { spark =>
      import spark.implicits._

      // For alternate-url-deltify:
      val alts = Seq(
        ("subdomain.hginsights.com", "hginsights.com", "alias"),
        ("hginsights.info",          "hginsights.com", "alias")
      ).toDF("alternate_url", "url", "alternate_url_type")
      alts.write.mode(SaveMode.Overwrite).parquet(AltUrlDeltify.inputPath)

      // For intent prep:
      val intentRaw = Seq(
        IntentFixtures.header,
        IntentFixtures.hgRow
      ).toDF
      intentRaw.repartition(1).write.mode(SaveMode.Overwrite).text(IntentPrep.inputPath)
    }
  }

  ignore("The Main entrypoint") {

    describe(s"running the ${AltUrlDeltify.command} command") {
      it ("should not explode catastrophically") {
        withTestSparkSysProps {
          Main.main(Array(
            AltUrlDeltify.command,
            "-i", AltUrlDeltify.inputPath,
            "-o", AltUrlDeltify.outputPath
          ))
          withTestSpark { implicit spark =>
            new Reader.HudiSnapshot(AltUrlDeltify.outputPath, 1).read.show()
          }
        }
      }
    }

    describe(s"running the ${MetroLookupDeltify.command} command") {
      it ("should not explode catastrophically") {
        withTestSparkSysProps {
          Main.main(Array(
            MetroLookupDeltify.command,
            "-o", MetroLookupDeltify.outputPath
          ))
          withTestSpark { implicit spark =>
            new Reader.HudiSnapshot(MetroLookupDeltify.outputPath, 1).read.show()
          }
        }
      }
    }

    describe(s"running the ${IntentPrep.command} command") {
      it ("should not explode catastrophically") {
        withTestSparkSysProps {
          Main.main(Array(
            IntentPrep.command,
            "-i", IntentPrep.inputPath,
            "-o", IntentPrep.outputPath
          ))
          withTestSpark { implicit spark =>
            new Reader.HudiSnapshot(IntentPrep.outputPath, 1).read.show()
          }
        }
      }
    }

    describe(s"running the ${IntentUpdate.command} command") {
      it ("should not explode catastrophically and produce expected data mapped correctly") {
        withTestSparkSysProps {
          Main.main(Array(
            IntentUpdate.command,
            "--input-alternate-urls-path", IntentUpdate.inputAlternateUrlsPath,
            "--input-metro-lookup-path", IntentUpdate.inputMetroLookupPath,
            "--input-prepped-intent-path", IntentUpdate.inputIntentPrepPath,
            "--input-prepped-intent-since", "19991231235959", // Y2K
            "-o", IntentUpdate.outputPath
          ))
          withTestSpark { implicit spark =>
            val out: DataFrame = new Reader.HudiSnapshot(IntentUpdate.outputPath, 1).read.cache
            out.show()
            assert(out.count() == 1)
            val record = out.head
            assert(
              record.getAs[String]("url") == "hginsights.com" && // ensure that domain is carried despit missing mapping
              record.getAs[String]("metro_area") == "santa barbara, california area" && // expected metro joiner
              record.getAs[String]("state") == "CA" // ensures that metro data mapped
            )
          }
        }
      }
    }
  }
}
