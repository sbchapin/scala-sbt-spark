package com.hgdata.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import collection.JavaConverters._

class SparkSessionManager(sparkConfigs: Map[String, String]) extends LazyLogging {

  /** A convenience closure to configure, start, provide, then shut down a SparkSession. */
  def withSpark(task: SparkSession => Unit): Unit = {
    // Set up Spark:
    val sparkBuilder: SparkSession.Builder = sparkConfigs.foldLeft(SparkSession.builder()) {
      case (builder, (key, value)) => builder.config(key, value)
    }
    val spark = sparkBuilder.getOrCreate()
    logger.debug(s"Spark session initialized as $spark")

    // Use Spark:
    task(spark)

    // Shut down Spark:
    spark.close()
    logger.debug(s"Spark session $spark stopped")
  }
}

/** Companion object */
object SparkSessionManager {
  def apply(sparkConfigs: Map[String, String] = Map.empty): SparkSessionManager = new SparkSessionManager(sparkConfigs)
  def apply(sparkConfigs: java.util.Map[String, String]): SparkSessionManager = {
    // convert and clean java map to scala:
    val scalaSparkConfigs = for {
      (key, maybeValue) <- sparkConfigs.asScala.toMap
      value <- Option(maybeValue)
      if value.nonEmpty
    } yield (key, value)
    new SparkSessionManager(scalaSparkConfigs)
  }


}
