package com.hgdata.spark.io

import org.apache.spark.sql.functions.{col, typedLit}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

trait Reader {
  def read: DataFrame
  def map(x: DataFrame => DataFrame): Reader = {
    val outer = this
    new Reader {
      override def read: DataFrame = x(outer.read)
    }
  }
}

object Reader {

  /** A collection of various factory functions that create readers. */
  class ReaderHelpers(inputPath: String) {

    /** CSV, customized for reading raw intent format. */
    def rawIntent(implicit s: SparkSession): Reader = new Reader.RawIntent(inputPath)
    /** Parquet, holistic, grabs the run ID from path */
    def altUrl(implicit s: SparkSession): Reader = new Reader.Parquet(inputPath)
      .map { _.withColumn("run_id", typedLit(Pathing.getDatePartition(inputPath).orNull)) }
  }


  /** Read a generic Parquet format from a path */
  class Parquet(path: String)(implicit spark: SparkSession) extends Reader {
    override def read: DataFrame = spark.read.parquet(path)
  }

  /** Read a highly-specific Intent CSV format from a path */
  class RawIntent(path: String)(implicit spark: SparkSession) extends Reader {

    /** Schema for CSV file, column order matters: */
    private val schema = StructType(Array(
      StructField("Company Name",          StringType, nullable = true),
      StructField("Domain",                StringType, nullable = true),
      StructField("Size",                  StringType, nullable = true),
      StructField("Industry",              StringType, nullable = true),
      StructField("Category",              StringType, nullable = true),
      StructField("Topic",                 StringType, nullable = true),
      StructField("Composite Score",       IntegerType, nullable = true),
      StructField("Metro Area",            StringType, nullable = true),
      StructField("Metro Composite Score", IntegerType, nullable = true),
      StructField("Domain Origin",         StringType, nullable = true),
      StructField("Date Stamp",            StringType, nullable = true),
      StructField("HVC Level1 Trend",      StringType, nullable = true)
    ))

    /** Turns all schema columns to snake_case: */
    private val columnMapping: Seq[Column] = for {
      field <- schema
      from  =  field.name
      to    =  field.name.replace(" ", "_").toLowerCase
    } yield col(from).as(to)

    override def read: DataFrame = {
      spark
        .read
        .option("header", "true")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("delimiter", ",")
        .schema(schema)
        .csv(path)
        .select(columnMapping:_* )
    }
  }

}
