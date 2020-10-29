package com.hgdata.spark

import java.util
import java.util.concurrent.Callable

import com.hgdata.generated.BuildInfo
import com.hgdata.spark
import com.hgdata.spark.io.{Reader, Writer}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import picocli.CommandLine
import picocli.CommandLine.{Command, Option}

@Command(
  name = BuildInfo.name,
  mixinStandardHelpOptions = true,
  version = Array(BuildInfo.version + " (" + BuildInfo.builtAtString + ")"),
  description = Array(BuildInfo.description)
)
object Main extends Runnable with LazyLogging {
  @Option(
    names = Array("-i", "--input"),
    required = true,
    description = Array("""Path to write input.  Can be any path your Spark installation supports, e.g. file, s3, hdfs, etc.""")
  )
  private[spark] var inputPath: String = _

  @Option(
    names = Array("-o", "--output"),
    required = true,
    description = Array("""Path to write output.  Can be any path your Spark installation supports, e.g. file, s3, hdfs, etc.""")
  )
  private[spark] var outputPath: String = _

  def main(args: Array[String]): Unit = {
    val exitCode = new CommandLine(this).execute(args:_*)
    if (exitCode != 0) throw new RuntimeException(s"Process exited with status code ${exitCode}")
  }

  @throws[Exception]
  override def run(): Unit = {
    SparkSessionManager().withSpark { implicit spark: SparkSession =>
      // I/O, lazily wired:
      lazy val rawIntentReader: Reader = new Reader.RawIntent(inputPath)
      lazy val preppedIntentWriter: Writer = new Writer.Parquet(outputPath)

      // Jobs, lazily wired:
      lazy val prep = new IntentPrep(rawIntentReader, preppedIntentWriter)

      prep.run()
    }
  }
}
