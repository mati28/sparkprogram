package net.atos.spark.config

import org.apache.spark.sql.DataFrame

object Utils {

  def getDF(path: String): DataFrame = {

    val df = Constant.spark.read.option("header", true).csv(path)
    df
  }

}
