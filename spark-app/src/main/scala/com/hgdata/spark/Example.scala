package com.hgdata.spark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Example {

  /**
    *  Read data from a path into a Spark dataframe, then write it out of that dataframe to a path.
    *
    *  Exercises all of Spark, the data transit, and the serialization therein.
    *
    * @param in    Spark-valid path to read in
    * @param out   Spark-valid path to write out
    * @param spark The spark session used to do any spark operations
    */
  def run(in: String, out: String)(implicit spark: SparkSession): Unit = {

    // Read in, using spark:
    val dataframe: DataFrame = spark.read.json(in)

    // Write out, using spark:
    dataframe.write.mode(SaveMode.Overwrite).json(out)
  }
}
