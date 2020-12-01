package com.hgdata.spark.io

import com.typesafe.scalalogging.LazyLogging
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}

trait Writer {
  def write(df: DataFrame): Unit
}

object Writer {

  object WriterHelpers {
    def countPartitions(partition: String): Int = partition.count(_ == '/') + 1

    val preppedIntentPartition: String = "date_stamp"
    val preppedIntentPartitionCount: Int = countPartitions(preppedIntentPartition)

    val alternateUrlsPartition: String = "default" // Single "default" partition, will be added as a column for hive to partition on
    val alternateUrlsPartitionCount: Int = countPartitions(alternateUrlsPartition)
  }

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
      partitionField = WriterHelpers.preppedIntentPartition,
      precombineField = "date_stamp",
      operation = HudiWriteOperation.BulkInsert
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
      partitionField = WriterHelpers.alternateUrlsPartition,
      precombineField = "run_id"
    )

    /** Hudi Hive, delta, keyed off `uuid` and `date_stamp` columns. */
    def intentInsertDelta: Writer = new Writer.HudiHive(
      path = path,
      database = hiveDatabase,
      table = "intent",
      idField = "uuid",
      partitionField = WriterHelpers.preppedIntentPartition,
      precombineField = "date_stamp",
      operation = HudiWriteOperation.Insert(
        splitSize = 3000000 // @ Roughly 2Bil intent per delivery, this makes 600 files
      )
    )

  }

  /**
    * Write generically to parquet path.
    *
    * @param path Where (local, dfs) to write the whole dataframe.
    */
  class Parquet(path: String) extends Writer with LazyLogging {

    logger.debug(s"Generic parquet writer ${this.getClass.getSimpleName} configured to output to ${path}")
    override def write(df: DataFrame): Unit = {
      df.write
        .mode(SaveMode.Overwrite)
        .parquet(path)
    }
  }

  sealed trait HudiWriteOperation {
    def configName: String
  }
  object HudiWriteOperation {
    case object Upsert extends HudiWriteOperation {
      override val configName: String = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL
    }
    case class Insert(splitSize: Long = HoodieCompactionConfig.DEFAULT_COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE.toLong) extends HudiWriteOperation {
      override val configName: String = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL
    }
    case object BulkInsert extends HudiWriteOperation {
      override val configName: String = DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL
    }
  }

  /**
    * Write generically to hudi and hive table.
    *
    * @param path Where (local, dfs) to write deltas to
    * @param table Name of the Hudi/Hive table to persist
    * @param idField Column name of unique primary key of the dataset
    * @param partitionField Column name used to partition data
    * @param precombineField Column used to precombine duplicates before insert
    * @param operation Which [[org.apache.hudi.DataSourceWriteOptions]] (bulk_insert, insert, or upsert) to use for write operation.
    * @param database What Hive database to write to, defaults to no database (and no hive support)
    */
  class HudiHive(path: String,
                 table: String,
                 idField: String,
                 partitionField: String,
                 precombineField: String,
                 operation: HudiWriteOperation,
                 database: Option[String] = None) extends Writer with LazyLogging {

    private val hasDefaultPartition = partitionField == "default"

    private val hudiOpts: Map[String, String] = Map(
      // Static:
      DataSourceWriteOptions.TABLE_TYPE_OPT_KEY -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL, // Copy On Write
      DataSourceWriteOptions.OPERATION_OPT_KEY -> operation.configName,
      // Dynamic:
      HoodieWriteConfig.TABLE_NAME -> table,
      DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> idField,
      DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> partitionField,
      DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> precombineField
    )

    private val maybeHudiInsertOpts: Option[Map[String, String]] = Some(operation).collect {
      case HudiWriteOperation.Insert(splitSize) => Map(
        HoodieCompactionConfig.COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE -> splitSize.toString
      )
    }

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

    private val opts: Map[String, String] =
      hudiOpts ++
        maybeHudiInsertOpts.getOrElse(Map.empty) ++
        maybeHiveOpts.getOrElse(Map.empty)

    logger.debug(s"""Generic hudi hive writer ${this.getClass.getSimpleName} configured to output to ${path} and ${table} with the following options: ${opts}""")

    override def write(df: DataFrame): Unit = {
      val output = if (hasDefaultPartition) {
        // This is necessary to allow Hive a partkey - otherwise presto (Athena) cannot query it.
        df.withColumn("default", lit("default"))
      } else {
        df
      }
      output
        .write
        .format("org.apache.hudi")
        .options(opts)
        .mode(SaveMode.Append)
        .save(Pathing.relativeToAbsolute(path))
    }
  }

  /**
    * Write the difference of a dataset generically to hudi and hive table.
    *
    * Expects the "whole data" for every invocation, __not the delta__. Will calculate the difference and upsert it.
    *
    * Differences are keyed by `idField` and updates are detected from a difference in equality of any `changeFields`.
    *
    * @param path Where (local, dfs) to write deltas to and to read snapshot from
    * @param table Name of the Hudi/Hive table to persist
    * @param idField Column name of unique primary key of the dataset
    * @param changeFields Any difference in row-level equality for any provided column names will propagate an update
    * @param partitionField Column name used to partition data
    * @param precombineField Column used to precombine duplicates before insert
    * @param beginInstant When to start reading the current-state snapshot for diffing, defaults to now, yyyymmddhhmmss
    * @param endInstant When to stop reading the current-state snapshot for diffing, defaults to now, yyyymmddhhmmss
    * @param database What Hive database to write to, defaults to no database (and no hive support)
    */
  class HudiHiveHolistic(path: String,
                         table: String,
                         idField: String,
                         changeFields: Seq[String],
                         partitionField: String,
                         precombineField: String,
                         beginInstant: Option[String] = None,
                         endInstant: Option[String] = None,
                         database: Option[String] = None)
                        (implicit spark: SparkSession) extends Writer with LazyLogging {

    /** Underlying reader for the dataset we are attempting to write to - always reads the holistic state, dictated by instant. */
    val reader = new Reader.HudiSnapshot(
      path = path,
      numPartitions = partitionField.count(_ == '/') + 1,
      beginInstant = beginInstant,
      endInstant = endInstant
    )

    /** Underlying writer for the dataset we are writing to - will persist any information issued. */
    def writer(operation: HudiWriteOperation) = new Writer.HudiHive(
      path = path,
      database = database,
      table = table,
      idField = idField,
      partitionField = partitionField,
      precombineField = precombineField,
      operation = operation
    )

    override def write(df: DataFrame): Unit = {
      // The holistic desired state and holistic current state of what exists
      val desired = df
      val current = try {
        reader.read
      } catch {
        case e: AnalysisException =>
          logger.warn(s"Failed to read from $reader - assuming this is the first time writing to that dataset; will initialize with bulk inserts.  Reader failure follows, though you can likely ignore it:", e)
          // Bulk insert everything
          writer(HudiWriteOperation.BulkInsert).write(desired)
          return // don't continue to deltas
      }

      // The (calculated) delta
      val upserts: DataFrame = desired
        .join(current, current(idField) === desired(idField), "full")
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
          // Coalesce all columns (to allow deleting off partition by id with precombination):
          desired.columns.map { c => coalesce(desired(c), current(c)).as(c) } :+
          // Add deletion marker (Hudi upserter uses this to delete, it must be nullable):
          when(desired(idField).isNull, true).otherwise(lit(null)).as("_hoodie_is_deleted"):_*
        )

      // Upsert (just) the delta
      writer(HudiWriteOperation.Upsert).write(upserts)
    }
  }
}
