package com.sbchapin.spark

import java.util
import java.util.concurrent.Callable

import com.sbchapin.generated.BuildInfo
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
object Main extends Callable[Integer] with LazyLogging {

  @Option(
    names = Array("--spark-master"),
    required = false,
    description = Array("""Master path to provide spark session when initializing spark. Consider "local[*]" if running standalone or instead "spark://IP:PORT" if remote, or simply leave blank if configuring from spark-submit.""")
  )
  private[spark] var sparkMaster: String = _

  @Option(
    names = Array("--spark-conf"),
    required = false,
    description = Array("""Extra config to pass to spark on init - mostly useful when running with --spark-master="local[*]", as you would otherwise usually pass these directly to spark-submit.  Repeat this parameter for each key=value.""")
  )
  private[spark] var sparkConfig: java.util.Map[String, String] = new util.HashMap()

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
    System.exit(new CommandLine(this).execute(args:_*))
  }

  @throws[Exception]
  override def call: Integer = {
    sparkConfig.put("spark.master", sparkMaster)
    SparkSessionManager(sparkConfig).withSpark { implicit spark: SparkSession =>
      Example.run(
        in = inputPath,
        out = outputPath
      )
    }
    0 // success
  }
}
