package com.hgdata.picocli

import com.hgdata.spark.io.Reader.ReaderHelpers
import picocli.CommandLine

/** A simple mixin to bring in --input args for jobs and lazily provide pre-wired various readers. */
trait InputCommandLineOpts {

  @CommandLine.Option(
    names = Array("-i", "--input"),
    required = true,
    description = Array("""Path to read input.  Can be any path your Spark installation supports, e.g. file, s3, hdfs, etc.""")
  )
  var inputPath: String = _

  lazy val readers = new ReaderHelpers(inputPath)
}
