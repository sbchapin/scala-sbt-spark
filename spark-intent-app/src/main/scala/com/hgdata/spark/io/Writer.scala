package com.hgdata.spark.io

import com.typesafe.scalalogging.LazyLogging
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.spark.sql.{DataFrame, SaveMode}

trait Writer {
  def write(df: DataFrame): Unit
}

object Writer {

  /** Write generically to parquet path */
  class Parquet(path: String)
    extends Writer
      with LazyLogging
  {
    logger.debug(s"Generic parquet writer ${this.getClass.getSimpleName} configured to output to ${path}")
    override def write(df: DataFrame): Unit = {
      df.write
        .mode(SaveMode.Overwrite)
        .parquet(path)
    }
  }

  /** Write generically to hudi and hive table */
  class HudiHive(path: String, table: String, idField: String, partitionField: String, precombineField: String, database: Option[String] = None)
    extends Writer
      with LazyLogging
  {
    private val hudiOpts: Map[String, String] = Map(
      // Static:
      DataSourceWriteOptions.TABLE_TYPE_OPT_KEY -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL, // Copy On Write
      DataSourceWriteOptions.OPERATION_OPT_KEY -> DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL, // Insert
      // Dynamic:
      HoodieWriteConfig.TABLE_NAME -> table,
      DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> idField,
      DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> partitionField,
      DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> precombineField
    )

    private val maybeHiveOpts: Option[Map[String, String]] = database.map { db =>
      Map(
        // Static:
        DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY -> "true",
        DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY -> classOf[MultiPartKeysValueExtractor].getName,
        DataSourceWriteOptions.HIVE_URL_OPT_KEY -> "jdbc:hive2://localhost:10000",
        // Dynamic:
        DataSourceWriteOptions.HIVE_TABLE_OPT_KEY -> table,
        DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY -> db,
        DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY -> partitionField
      )
    }

    private val opts: Map[String, String] = maybeHiveOpts.foldLeft(hudiOpts) { _ ++ _ }

    logger.debug(s"""Generic hudi hive writer ${this.getClass.getSimpleName} configured to output to ${path} and ${table} with the following options: ${opts}""")

    override def write(df: DataFrame): Unit = {
      df.write
        .format("org.apache.hudi")
        .options(opts)
        .mode(SaveMode.Append)
        .save(path)
    }
  }
}