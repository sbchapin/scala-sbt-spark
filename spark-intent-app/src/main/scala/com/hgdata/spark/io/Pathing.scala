package com.hgdata.spark.io

import java.nio.file.{Files, Paths}

import scala.util.matching.Regex

/** Tools to help manipulate paths related to the run of the pipeline. */
object Pathing {

  private val datePartMatcher: Regex = """(?x) # allow multiline regex
    [^\/]*                         # match the path segment
    (?<year>\d{4})                 # match year
    (?<separator>[-\/._]?)         # match various y-m-d separators
    (?<month>0[1-9]|1[012])       # match month
    \k<separator>                  # backreference, match the same separator
    (?<day>0[1-9]|[12][0-9]|3[01]) # match day
    [^\/]*                         # match the remainder of the path segment
  """.r

  /**
    * Try to get the first valid "date partiton" from a given path string.
    *
    * @example Examples of valid date paths to extract:<pre>`
    * s3://example/2020-01-01/
    * s3://example/20200101000000/
    * s3://example/prefix_2020_01_01/
    * s3://example/2020/01/01-suffix/
    * ./20200101/
    * /root/2020_01_01
    * `
    */
  def getDatePartition(path: String): Option[String] = datePartMatcher.findFirstIn(path)

  /**
    * Resolve a relative path to an absolute path (if it is a relative path, otherwise no-op.)
    *
    * @example `./relative` -> `/absolute/to/relative`
    */
  def relativeToAbsolute(path: String): String = {
    if (path.startsWith("./")) {
      Paths.get(path).toAbsolutePath.toString
    } else {
      path
    }
  }
}
