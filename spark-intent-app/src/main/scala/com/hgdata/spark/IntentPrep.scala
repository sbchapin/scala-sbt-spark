package com.hgdata.spark

import com.hgdata.spark.io.{Reader, Writer}
import org.apache.spark.sql.SparkSession

class IntentPrep(reader: Reader, writer: Writer)(implicit spark: SparkSession) {

  /** Basic pass-through for now (without any cleaning or transformation) */
  def run(): Unit = {
    val intent = reader.read
    writer.write(intent)
  }
}
