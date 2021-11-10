package com.hgdata.spark.io

import java.time.Instant

import com.hgdata.generated.BuildInfo
import com.hgdata.picocli.ITypeConverters.HudiInstant
import org.apache.hudi.DataSourceReadOptions
import org.apache.spark.sql.functions.{col, typedLit, lit}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, DataFrameReader, SparkSession}

import scala.io.Source

trait Reader {
  def read: DataFrame
  def map(x: DataFrame => DataFrame): Reader = {
    val outer = this
    new Reader {
      override def read: DataFrame = x(outer.read)
    }
  }
}

/** A more specific behavior of reader to support typing behaviors more strictly to read JUST the difference */
trait DeltaReader extends Reader

/** A more specific behavior of reader to support typing behaviors more strictly to read the entire state */
trait HolisticReader extends Reader

object Reader {

  /** A collection of various factory functions that create readers. */
  class ReaderHelpers(inputPath: String) {

    /** Hudi, holistic snapshot, (all) latest alternate URLs. */
    def allAlternateUrls(implicit spark: SparkSession): HolisticReader = new Reader.HudiSnapshot(
      path = inputPath,
      numPartitions = Writer.WriterHelpers.alternateUrlsPartitionCount
    )

    /** Derivative of Hudi, holistic snapshot, (all) latest URL aliases (alternate URLs of type "alias"). */
    def allAliasUrls(implicit spark: SparkSession): HolisticReader = new HolisticReader {
      override def read: DataFrame = {
        import spark.implicits._
        allAlternateUrls
          .read
          .where('alternate_url_type === "alias")
      }
    }

    /** Hudi, holistic snapshot, (all) latest metro lookup. */
    def allMetroLookup(implicit spark: SparkSession): HolisticReader = new Reader.HudiSnapshot(
      path = inputPath,
      numPartitions = Writer.WriterHelpers.metroLookupPartitionCount
    )

    /** Hudi, holistic snapshot, (all) latest prepped intent. */
    def allPreppedIntent(implicit spark: SparkSession): HolisticReader = new Reader.HudiSnapshot(
      path = inputPath,
      numPartitions = Writer.WriterHelpers.preppedIntentPartitionCount
    )

    /** Hudi, delta incremental snapshot, (delta) latest of any dataset at an input path from a specified instant onwards. */
    def deltaHudi(since: Instant)(implicit spark: SparkSession): DeltaReader = new Reader.HudiIncremental(
      path = inputPath,
      beginInstant = HudiInstant.format(since)
    )

    /** CSV, customized for reading raw intent format. */
    def rawIntent(implicit s: SparkSession): Reader = new Reader.RawIntent(inputPath)

    /** Parquet, holistic, grabs the run ID from path */
    def altUrl(implicit s: SparkSession): Reader = new Reader.Parquet(inputPath)
      .map { _.withColumn("run_id", typedLit(Pathing.getDatePartition(inputPath).orNull)) }

    /** CSV, holistic, uses locally-versioned src/main/resources/metro_lookup.json  */
    def metroLookup(implicit s: SparkSession): Reader = new Reader.MetroLookup
  }


  /**
    * Read a generic Parquet format from a path.
    *
    * @param path Where (local, dfs) to read from
    */
  class Parquet(path: String)(implicit spark: SparkSession) extends Reader {
    override def read: DataFrame = spark.read.parquet(path)
  }

  /**
    * Read a generic Hudi table from a path, incrementally (showing only deltas).
    * Given a timestamp, will return record changes since that time.
    *
    * @param path Where (local, dfs) to read from
    * @param beginInstant When to start incrementally reading from, yyyymmddhhmmss
    * @param endInstant When to stop incrementally reading from, defaults to now, yyyymmddhhmmss
    * @param pathGlobKey A prefix to help limit the partitions to read from
    */
  class HudiIncremental(path: String,
                        beginInstant: String,
                        endInstant: Option[String] = None,
                        pathGlobKey: Option[String] = None)
                       (implicit spark: SparkSession) extends DeltaReader {
    override def read: DataFrame = {
      val options: Map[String, Option[String]] = Map(
        DataSourceReadOptions.QUERY_TYPE_OPT_KEY -> Some(DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL),
        DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY -> Some(beginInstant),
        DataSourceReadOptions.END_INSTANTTIME_OPT_KEY -> endInstant,
        DataSourceReadOptions.INCR_PATH_GLOB_OPT_KEY -> pathGlobKey
      )
      val builder: DataFrameReader = options.foldLeft(spark.read.format("hudi")) {
        case (builder, (k, Some(v))) => builder.option(k, v)
        case (builder, (_, None))    => builder
      }
      // Load from configured dataframe reader builder:
      builder.load(Pathing.relativeToAbsolute(path))
    }
  }


  /**
    * Read a generic Hudi table from a path, at its latest time (showing the full state).
    * Must also provide the number of partitions it is stored under.
    *
    * @param path Where (local, dfs) to read from
    * @param numPartitions How many partitions the hudi table has (typically 1)
    * @param beginInstant When to start reading the snapshot from, defaults to now, yyyymmddhhmmss
    * @param endInstant When to stop reading the snapshot from, defaults to now, yyyymmddhhmmss
    */
  class HudiSnapshot(path: String,
                     numPartitions: Int,
                     beginInstant: Option[String] = None,
                     endInstant: Option[String] = None)
                    (implicit spark: SparkSession) extends HolisticReader {
    override def read: DataFrame = {
      val options: Map[String, Option[String]] = Map(
        DataSourceReadOptions.QUERY_TYPE_OPT_KEY -> Some(DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL),
        DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY -> beginInstant,
        DataSourceReadOptions.END_INSTANTTIME_OPT_KEY -> endInstant
      )
      val builder: DataFrameReader = options.foldLeft(spark.read.format("hudi")) {
        case (builder, (k, Some(v))) => builder.option(k, v)
        case (builder, (_, None))    => builder
      }
      val globs: String = (0 to numPartitions).map(_ => "/*").reduce(_ + _)
      // Load from configured dataframe reader builder (with hudi path globbing):
      builder.load(Pathing.relativeToAbsolute(path).stripSuffix("/") + globs)
    }
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

  /** Read a generic metro JSON format from a path, adding the version of this Jar */
  class MetroLookup(implicit spark: SparkSession) extends Reader {
    override def read: DataFrame = {
      val metroJsons: Seq[String] = Source.fromInputStream(getClass.getResourceAsStream("/metro_lookup.json")).getLines.toSeq
      import spark.implicits._
      spark
        .read
        .json(metroJsons.toDS)
        .withColumn("metro_version", lit(BuildInfo.version)) // Appends this jar's version on each row for traceability
    }
  }

}
