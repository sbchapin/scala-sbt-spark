package com.hgdata.spark.io

import scala.util.matching.Regex

/** Tools to help manipulate paths related to the run of the pipeline. */
object Runpath {
  private val datePartMatcher: Regex = """(?x) # allow multiline regex
    [^\/]*                         # match the path segment
    (?<year>\d{4})                 # match year
    (?<separator>[-\/._]?)         # match various y-m-d separators
    (?<month>0[1-9]|1[012])       # match month
    \k<separator>                  # backreference, match the same separator
    (?<day>0[1-9]|[12][0-9]|3[01]) # match day
    [^\/]*                         # match the remainder of the path segment
  """.r

  def getDatePartition(path: String): Option[String] = datePartMatcher.findFirstIn(path)
}
