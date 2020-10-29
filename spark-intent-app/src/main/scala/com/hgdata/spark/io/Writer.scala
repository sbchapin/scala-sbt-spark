package com.hgdata.spark.io

import org.apache.spark.sql.DataFrame

trait Writer {
  def write(df: DataFrame): Unit
}

object Writer {

  /** Write generically to parquet path */
  class Parquet(path: String) extends Writer {
    override def write(df: DataFrame): Unit = {
      df.write.parquet(path)
    }
  }
}
