package com.hgdata.spark.io

import com.hgdata.spark
import com.hgdata.spark.io
import com.typesafe.scalalogging.LazyLogging
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

trait Writer {
  def write(df: DataFrame): Unit
}

object Writer {

  /** A collection of various factory functions that create writers. */
  class WriterHelpers(path: String, hiveDatabase: Option[String] = None) {

    /** Parquet, holistic. */
    def preppedIntentWriter: Writer = new Writer.Parquet(path)

    /** Hudi Hive, delta, keyed off `uuid` and `date_stamp` columns. */
    def preppedIntentDelta: Writer = new Writer.HudiHive(
      path = path,
      database = hiveDatabase,
      table = "intent_prepped",
      idField = "uuid",
      partitionField = "date_stamp",
      precombineField = "date_stamp",
      operation = DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL // Bulk Insert
    )

    /**
      * Hudi Hive, delta, keyed off `alternate_url` column.
      * Takes a holistic alternate URL dataset and calculates deltas to update, including deletes and inserts.
      * Always operates off the latest snapshot.
      */
    def alternateUrlDelta(implicit spark: SparkSession): Writer = new Writer.HudiHiveHolistic(
      path = path,
      database = hiveDatabase,
      table = "alternate_urls",
      idField = "alternate_url",
      changeFields = Seq("url", "alternate_url_type"),
      partitionField = "", // Single "default" partition
      precombineField = "run_id"
    )
  }

  /** Write generically to parquet path */
  class Parquet(path: String) extends Writer with LazyLogging {

    logger.debug(s"Generic parquet writer ${this.getClass.getSimpleName} configured to output to ${path}")
    override def write(df: DataFrame): Unit = {
      df.write
        .mode(SaveMode.Overwrite)
        .parquet(path)
    }
  }

  /** Write generically to hudi and hive table */
  class HudiHive(path: String,
                 table: String,
                 idField: String,
                 partitionField: String,
                 precombineField: String,
                 operation: String,
                 database: Option[String] = None) extends Writer with LazyLogging {

    private val hudiOpts: Map[String, String] = Map(
      // Static:
      DataSourceWriteOptions.TABLE_TYPE_OPT_KEY -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL, // Copy On Write
      DataSourceWriteOptions.OPERATION_OPT_KEY -> operation,
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
        .save(Pathing.relativeToAbsolute(path))
    }
  }

  class HudiHiveHolistic(path: String,
                         table: String,
                         idField: String,
                         changeFields: Seq[String],
                         partitionField: String,
                         precombineField: String,
                         beginInstant: Option[String] = None,
                         endInstant: Option[String] = None,
                         database: Option[String] = None)
                        (implicit spark: SparkSession) extends Writer {

    /** Underlying reader for the dataset we are attempting to write to - always reads the holistic state, dictated by instant. */
    val reader = new Reader.HudiSnapshot(
      path = path,
      numPartitions = partitionField.count(_ == '/') + 1,
      beginInstant = beginInstant,
      endInstant = endInstant
    )

    /** Underlying writer for the dataset we are writing to - will persist any and all upserts issued. */
    val writer = new Writer.HudiHive(
      path = path,
      database = database,
      table = table,
      idField = idField,
      partitionField = partitionField,
      precombineField = precombineField,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL
    )

    override def write(df: DataFrame): Unit = {
      // The holistic desired state and holistic current state of what exists
      val desired = df
      val current = reader.read

      // The (calculated) delta
      val upserts: DataFrame = desired.join(current, current(idField) === desired(idField), "full")
        .where(
          // Detect net new (inserts):
          current(idField).isNull ||
          // Detect net loss (deletions):
          desired(idField).isNull ||
          // Detect net change (updates):
          changeFields
            .map { f => current(f) =!= desired(f) } // singular field has changed
            .reduce { _ || _ } // any field
        )
        .select(
          // Coalesce all columns to allow deleting off partition by id with precombination:
          desired.columns.map { c => coalesce(desired(c), current(c)).as(c) } :+
          // Add deletion marker that Hudi upserter uses to delete (must be nullable):
          when(desired(idField).isNull, true).otherwise(lit(null)).as("_hoodie_is_deleted"):_*
        )

      // Write (just) the delta
      writer.write(upserts)
    }
  }
}
