package com.hgdata.picocli

import com.hgdata.spark.io.Writer.WriterHelpers
import picocli.CommandLine

trait OutputCommandlineOpts {

  @CommandLine.Option(
    names = Array("-o", "--output"),
    required = true,
    description = Array("""Path to write output.  Can be any path your Spark installation supports, e.g. file, s3, hdfs, etc.""")
  )
  var outputPath: String = _

  @CommandLine.Option(
    names = Array("--output-database"),
    required = false,
    description = Array("""Hive database to use to persist & fetch schema for output dataset. If not provided, will not use hive.""")
  )
  var outputHiveDatabase: String = _

  lazy val writers = new WriterHelpers(outputPath, Option(outputHiveDatabase))
}
