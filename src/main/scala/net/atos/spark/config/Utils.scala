package net.atos.spark.config

import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs.{FileSystem,Path}

object Utils {

  /**
   * Read a dataframe and rename the columns
   * @param path
   * @return
   */
  def getDF(path: String): DataFrame = {
    val df:DataFrame = Constant.spark.read.json(path)
    val cols = df.columns.map(colNameConverter)
    df.toDF(cols:_*)
  }

  /**
   * This function write a dataframe to hdfs. If path already exists
   * it is overwritten otherwise it is written
   * @param df
   * @param path
   */
  def saveDF(df: DataFrame,path:String):Unit = {
    val fs = FileSystem.get(Constant.spark.sparkContext.hadoopConfiguration)

    //val isDir = new File(path).exists()
    val isDir = fs.exists(new Path(path))
    if(!isDir) {
      df.coalesce(1).write.parquet(path)

    } else {
      df.coalesce(1).write.mode("overwrite").parquet(path)
    }


  }

  /**
   * Renaming logic goes down here
   * @param colName
   * @return
   */
  def colNameConverter(colName: String):String = {
    val a = colName.split("_")
    var outPut = a(0)
    for(i <-(1 until a.size)){
      outPut += a(i).substring(0,1).toUpperCase() + a(i).substring(1,a(i).length)
    }
    outPut
  }

}
