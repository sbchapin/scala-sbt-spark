package com.hgdata.spark

import java.time.Instant

import com.hgdata.generated.BuildInfo
import com.hgdata.picocli.{InputCommandLineOpts, InstantString, OutputCommandlineOpts}
import com.hgdata.spark.io.Reader
import com.hgdata.spark.runnables.{IntentPrep, IntentUpdate, Passthrough}
import org.apache.hudi.DataSourceWriteOptions
import org.apache.spark.sql.SparkSession
import picocli.CommandLine

@CommandLine.Command(
  name = BuildInfo.name,
  mixinStandardHelpOptions = true,
  version = Array(BuildInfo.version + " (" + BuildInfo.builtAtString + ")"),
  description = Array(BuildInfo.description)
)
object Main {

  @CommandLine.Command(
    name = "intent-prep",
    description = Array(
      "Ingest a delivery of raw intent data into the 'Intent Prepped' delta table.",
      "Will only ever append to the delta table (insert only)."
    )
  )
  object IntentPrepSubcommand extends SparkRunnable with InputCommandLineOpts with OutputCommandlineOpts {
    override def run(): Unit = withDefaultSpark { implicit spark: SparkSession =>
      val prep = new IntentPrep(
        reader = readers.rawIntent,
        writer = writers.preppedIntentDelta
      )
      prep.run()
    }
  }

  @CommandLine.Command(
    name = "alternate-url-deltify",
    description = Array(
      "Ingest the latest state of Alternate URLs from the pipeline, persisting to the Alternate URLs delta table.",
      "Will persist only the difference (inserts, updates, and deletes)."
    )
  )
  object AlternateUrlPrepSubcommand extends SparkRunnable with InputCommandLineOpts with OutputCommandlineOpts {
    override def run(): Unit = withDefaultSpark { implicit spark: SparkSession =>
      val deltify = new Passthrough(
        reader = readers.altUrl,
        writer = writers.alternateUrlDelta
      )
      deltify.run()
    }
  }


  @CommandLine.Command(
    name = "intent-update",
    description = Array(
      "Ingest any net new prepped intent data, enriching it with the latest Alternate URLs.",
      "Typically run after ingesting new intent data into 'Intent Prepped'.",
      "Will persist updates to intent rows if run on a before-processed dataset, otherwise will add new records."
    )
  )
  object IntentUpdateSubcommand extends SparkRunnable with OutputCommandlineOpts {
    @CommandLine.Option(
      names = Array("--input-alternate-urls-path"),
      required = true,
      description = Array("""Path to read input for alternate url hudi table.  Can be any path your Spark installation supports, e.g. file, s3, hdfs, etc.""")
    )
    var alternateUrlInputPath: String = _

    @CommandLine.Option(
      names = Array("--input-prepped-intent-path"),
      required = true,
      description = Array("""Path to read input for prepped intent hudi table.  Can be any path your Spark installation supports, e.g. file, s3, hdfs, etc.""")
    )
    var preppedIntentInputPath: String = _

    @CommandLine.Option(
      names = Array("--input-prepped-intent-since"),
      required = true,
      description = Array("""At what point in time to read input from prepped intent hudi table.  Must be a valid yyyyMMddHHmmSS format."""),
      converter = Array(classOf[InstantString])
    )
    var preppedIntentInputSince: Instant = _

    private lazy val intentReaders = new Reader.ReaderHelpers(preppedIntentInputPath)
    private lazy val urlReaders = new Reader.ReaderHelpers(alternateUrlInputPath)

    override def run(): Unit = withDefaultSpark { implicit spark: SparkSession =>
      val update = new IntentUpdate(
        preppedIntentReader = intentReaders.newPreppedIntent(preppedIntentInputSince),
        alternateUrlReader = urlReaders.allAlternateUrls,
        writer = writers.intentInsertDelta
      )
      update.run()
    }
  }

  private[spark] lazy val commandLine: CommandLine =
    new CommandLine(this)
      .addSubcommand(IntentPrepSubcommand)
      .addSubcommand(AlternateUrlPrepSubcommand)
      .addSubcommand(IntentUpdateSubcommand)

  def main(args: Array[String]): Unit = {
    val exitCode = commandLine.execute(args:_*)
    if (exitCode != 0) throw new RuntimeException(s"Process exited with status code ${exitCode}")
  }
}
