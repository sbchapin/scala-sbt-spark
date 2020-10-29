package com.hgdata.spark.testutil

import com.hgdata.spark.SparkSessionManager
import org.apache.spark.sql.SparkSession

trait SparkHelpers {

  private val defaultTestConfig = Map(
    "spark.master" -> "local[*]",
    "spark.default.parallelism" -> "8"
  )
  private val sparkSessionManager = SparkSessionManager(defaultTestConfig)

  /** Provide a spark context to a given test. */
  def withTestSpark(task: SparkSession => Unit): Unit = sparkSessionManager.withSpark(task)
}
