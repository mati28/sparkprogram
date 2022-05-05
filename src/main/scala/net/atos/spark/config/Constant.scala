package net.atos.spark.config
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession


object Constant extends Serializable {

    lazy val conf: Config = ConfigFactory.load()

    val PRODUCTS: String = conf.getString("APP.INPUTTABLE.PRODUCTS")
    val ORDERITEMS: String = conf.getString("APP.INPUTTABLE.ORDERITEMS")
  val CATEGORIES: String = conf.getString("APP.INPUTTABLE.CATEGORIES")
  val DEPARTMENTS: String = conf.getString("APP.INPUTTABLE.DEPARTMENTS")
    val APPNAME : String = conf.getString("APP.NAME")
    val MASTER: String = conf.getString("APP.MASTER")

    val spark: SparkSession =
      SparkSession.
        builder().
        appName(APPNAME).
        master(MASTER).
        //.enableHiveSupport()
        getOrCreate()

    lazy val orderSchema = StructType(List(
      StructField("orderId",IntegerType),
      StructField("orderDate",StringType),
      StructField("orderCustomerId",IntegerType),
      StructField("orderStatus",StringType)))

    lazy val customerSchema = StructType(List(
      StructField("customerId",IntegerType,true),
      StructField("customerFirstName",StringType,true),
      StructField("customerLastName",StringType,true),
      StructField("customerEmail",StringType,true),
      StructField("customerStreet",StringType,true),
      StructField("customerCity",StringType,true),
      StructField("customerState",StringType,true),
      StructField("customerZipcode",StringType,true)))

}
