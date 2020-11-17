package com.hgdata.spark

import com.hgdata.generated.BuildInfo
import com.hgdata.spark.io.{Reader, Writer}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import picocli.CommandLine

@CommandLine.Command(
  name = BuildInfo.name,
  mixinStandardHelpOptions = true,
  version = Array(BuildInfo.version + " (" + BuildInfo.builtAtString + ")"),
  description = Array(BuildInfo.description)
)
object Main extends Runnable with LazyLogging {
  @CommandLine.Option(
    names = Array("-i", "--input"),
    required = true,
    description = Array("""Path to write input.  Can be any path your Spark installation supports, e.g. file, s3, hdfs, etc.""")
  )
  private[spark] var inputPath: String = _

  @CommandLine.Option(
    names = Array("-o", "--output"),
    required = true,
    description = Array("""Path to write output.  Can be any path your Spark installation supports, e.g. file, s3, hdfs, etc.""")
  )
  private[spark] var outputPath: String = _

  @CommandLine.Option(
    names = Array("--database"),
    required = false,
    description = Array("""Hive database to use to persist & fetch schema. If not provided, will not use hive.""")
  )
  private[spark] var hiveDatabase: String = _

  private[spark] val defaultConfig = Map(
    "spark.sql.hive.convertMetastoreParquet" -> "false",
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer"
  )

  def main(args: Array[String]): Unit = {
    val exitCode = new CommandLine(this).execute(args:_*)
    if (exitCode != 0) throw new RuntimeException(s"Process exited with status code ${exitCode}")
  }

  @throws[Exception]
  override def run(): Unit = {
    SparkSessionManager(defaultConfig).withSpark { implicit spark: SparkSession =>
      // I/O, lazily wired:
      lazy val rawIntentReader: Reader = new Reader.RawIntent(inputPath)
      lazy val preppedIntentWriter: Writer = new Writer.Parquet(outputPath)
      lazy val preppedIntentDeltaWriter: Writer = new Writer.HudiHive(
        path = outputPath,
        database = Option(hiveDatabase),
        table = "intent_prepped",
        idField = "uuid",
        partitionField = "date_stamp",
        precombineField = "date_stamp"
      )

      // Jobs, lazily wired:
      lazy val prep = new IntentPrep(rawIntentReader, preppedIntentDeltaWriter)

      prep.run()
    }
  }
}
