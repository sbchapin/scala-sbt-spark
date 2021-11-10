package com.hgdata.spark

import java.time.Instant

import com.hgdata.generated.BuildInfo // if this is unhappy, `sbt compile` to generate anew
import com.hgdata.picocli.{ITypeConverters, InputCommandLineOpts, OutputCommandlineOpts}
import com.hgdata.spark.io.Reader
import com.hgdata.spark.io.Reader.ReaderHelpers
import com.hgdata.spark.runnables.{IntentPrep, IntentUpdate, Passthrough}
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
    name = "metro-lookup-deltify",
    description = Array(
      "Ingest the latest state of the metro lookup, persisting to the metro lookup delta table.",
      "Will persist only the difference (inserts, updates, and deletes)."
    )
  )
  object MetroLookupPrepSubcommand extends SparkRunnable with OutputCommandlineOpts {
    lazy val readers = new ReaderHelpers(null)
    override def run(): Unit = withDefaultSpark { implicit spark: SparkSession =>
      val deltify = new Passthrough(
        reader = readers.metroLookup,
        writer = writers.metroLookupDelta
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
      names = Array("--input-metro-lookup-path"),
      required = true,
      description = Array("""Path to read input for metro lookup hudi table.  Can be any path your Spark installation supports, e.g. file, s3, hdfs, etc.""")
    )
    var metroLookupInputPath: String = _

    @CommandLine.Option(
      names = Array("--input-prepped-intent-path"),
      required = true,
      description = Array("""Path to read input for prepped intent hudi table.  Can be any path your Spark installation supports, e.g. file, s3, hdfs, etc.""")
    )
    var preppedIntentInputPath: String = _

    @CommandLine.Option(
      names = Array("--input-prepped-intent-since"),
      required = true,
      description = Array("""At what point in time to read input from prepped intent hudi table.  Must be a valid yyyyMMddHHmmss, iso zoned timestamp, or yyyy-mm-dd format."""),
      converter = Array(classOf[ITypeConverters.LenientInstant])
    )
    var preppedIntentInputSince: Instant = _

    private lazy val intentReaders = new Reader.ReaderHelpers(preppedIntentInputPath)
    private lazy val urlReaders = new Reader.ReaderHelpers(alternateUrlInputPath)
    private lazy val metroReaders = new Reader.ReaderHelpers(metroLookupInputPath)

    override def run(): Unit = withDefaultSpark { implicit spark: SparkSession =>
      val update = new IntentUpdate(
        preppedIntentReader = intentReaders.deltaHudi(preppedIntentInputSince),
        aliasUrlReader = urlReaders.allAliasUrls,
        metroLookupReader = metroReaders.allMetroLookup,
        writer = writers.intentInsertDelta
      )
      update.run()
    }
  }

  private[spark] lazy val commandLine: CommandLine =
    new CommandLine(this)
      .addSubcommand(IntentPrepSubcommand)
      .addSubcommand(AlternateUrlPrepSubcommand)
      .addSubcommand(MetroLookupPrepSubcommand)
      .addSubcommand(IntentUpdateSubcommand)

  def main(args: Array[String]): Unit = {
    val exitCode = commandLine.execute(args:_*)
    if (exitCode != 0) throw new RuntimeException(s"Process exited with status code ${exitCode}")
  }
}
