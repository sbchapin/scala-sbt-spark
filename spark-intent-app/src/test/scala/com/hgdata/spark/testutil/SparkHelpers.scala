package com.hgdata.spark.testutil

import com.hgdata.spark.SparkSessionManager
import org.apache.spark.sql.SparkSession

trait SparkHelpers {

  private val sparkSessionManager = SparkSessionManager(Map("spark.master" -> "local[*]"))

  /** Provide a spark context to a given test. */
  def withTestSpark(task: SparkSession => Unit): Unit = sparkSessionManager.withSpark(task)
}
