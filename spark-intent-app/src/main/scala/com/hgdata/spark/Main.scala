package com.hgdata.spark

import com.hgdata.generated.BuildInfo
import com.hgdata.picocli.{InputCommandLineOpts, OutputCommandlineOpts}
import org.apache.spark.sql.SparkSession
import picocli.CommandLine

@CommandLine.Command(
  name = BuildInfo.name,
  mixinStandardHelpOptions = true,
  version = Array(BuildInfo.version + " (" + BuildInfo.builtAtString + ")"),
  description = Array(BuildInfo.description)
)
object Main extends Runnable with InputCommandLineOpts with OutputCommandlineOpts {

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
      // Jobs, lazily wired:
      lazy val prep = new IntentPrep(
        reader = readers.rawIntentReader,
        writer = writers.preppedIntentDeltaWriter
      )

      prep.run()
    }
  }
}
