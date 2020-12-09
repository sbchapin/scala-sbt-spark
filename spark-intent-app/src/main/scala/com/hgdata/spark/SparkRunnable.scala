package com.hgdata.spark

import org.apache.spark.sql.SparkSession

trait SparkRunnable extends Runnable {

  private[spark] val defaultConfig = Map(
    "spark.sql.hive.convertMetastoreParquet" -> "false",
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer"
  )

  /** Helper method for providing a managed default spark session */
  protected def withDefaultSpark: (SparkSession => Unit) => Unit = {
    SparkSessionManager(defaultConfig).withSpark _
  }

}
