package com.hgdata.spark

import com.hgdata.spark.io.{Reader, Writer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class IntentPrep(reader: Reader, writer: Writer)(implicit spark: SparkSession) {

  /** Basic pass-through for now (without any cleaning or transformation) */
  def run(): Unit = {
    import spark.implicits._
    val intent = reader.read
      .filter($"date_stamp".isNotNull)
      .withColumn("uuid", hash($"company_name", $"domain", $"category", $"topic", $"metro_area", $"domain_origin", $"date_stamp"))
    writer.write(intent)
  }
}
