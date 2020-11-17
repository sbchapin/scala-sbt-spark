package com.hgdata.spark.testutil

import com.hgdata.spark.SparkSessionManager
import org.apache.spark.sql.SparkSession

trait SparkHelpers {

  private val defaultTestConfig = Map(
    "spark.master" -> "local[*]",
    "spark.default.parallelism" -> "8",
    // Hudi configs:
    "spark.sql.hive.convertMetastoreParquet" -> "false",
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "hoodie.bulkinsert.shuffle.parallelism" -> "8",
    "hoodie.insert.shuffle.parallelism" -> "8",
    "hoodie.upsert.shuffle.parallelism" -> "8",
    "hoodie.delete.shuffle.parallelism" -> "8",
    "hoodie.rollback.parallelism" -> "8",
    "hoodie.finalize.write.parallelism" -> "8",
    "hoodie.bloom.index.parallelism" -> "8"
  )
  private val sparkSessionManager = SparkSessionManager(defaultTestConfig)

  /** Provide a spark context to a given test. */
  def withTestSpark(task: SparkSession => Unit): Unit = sparkSessionManager.withSpark(task)

  /** Set the system properties to configure a test spark harness within a given context, then reset them. */
  def withTestSparkSysProps(task: => Unit): Unit = {

    // Get the existing system properties
    val cachedProps: Map[String, String] =  for ((k, v) <- defaultTestConfig) yield k -> System.getProperty(k)

    // Override values and execute test
    for ( (k, v) <- defaultTestConfig ) {
      System.setProperty(k, v)
    }
    try {
      task
    } finally {
      // Reset back to saved values
      cachedProps.foreach {
        case (k, null) => System.clearProperty(k)
        case (k, v) =>    System.setProperty(k, v)
      }
    }
  }
}
