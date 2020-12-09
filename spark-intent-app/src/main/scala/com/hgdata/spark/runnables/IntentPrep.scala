package com.hgdata.spark.runnables

import com.hgdata.spark.io.{Reader, Writer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}

class IntentPrep(reader: Reader, writer: Writer)(implicit spark: SparkSession) extends Runnable {

  private def generateUUID(cols: Column*): Column = {
    val hashed8Cols =
      cols
        .map(md5)
        .map(substring(_, 0, 8))
    concat_ws("-", hashed8Cols:_*)
  }

  /** Basic pass-through for now (without any cleaning or transformation) */
  override def run(): Unit = {
    import spark.implicits._
    // Columns used to determine uniqueness of record (order matters)
    val uuidCols = Seq($"date_stamp", $"company_name", $"domain", $"category", $"topic", $"metro_area", $"domain_origin")
    // Filtered null records with uniqueness
    val preppedIntent = reader.read
      .filter($"date_stamp".isNotNull)
      .withColumn("uuid", generateUUID(uuidCols:_*))
    // Persist
    writer.write(preppedIntent)
  }
}
