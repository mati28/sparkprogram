package net.atos.spark.service

import net.atos.spark.config.Constant
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType


object LogicBase {

    val spark = Constant.spark
  import spark.implicits._

  val productsDF = spark.read.option("header",true).
    csv(Constant.PRODUCTS)
  val orderItemsDF = spark.read.option("header",true).option("inferSchema",true).
    csv(Constant.ORDERITEMS)
  val departmentsDF = spark.read.option("header",true).
    csv(Constant.DEPARTMENTS)
  val categoriesDF = spark.read.option("header",true).
    csv(Constant.CATEGORIES)

  def mostSoldItems() = {

    val mostSold = productsDF.join(orderItemsDF,'productId <=> 'orderItemProductId).
      groupBy("productId","productName").
      agg(
        count("productId").as("countItemsSold")
      ).sort('countItemsSold.desc).
      withColumn("countItemsSold",format_number('countItemsSold,0))
    mostSold.coalesce(1).write.parquet("/data/bigdata/data/retail_db/countofsolditems_by_department")

  }

  def revenueByDepartment = {

    val df = departmentsDF.join(categoriesDF,'departmentId <=> 'categoryDepartmentId).
      join(productsDF,'categoryId <=> 'productCategoryId).
      join(orderItemsDF,'productId <=> 'orderItemProductId).groupBy("departmentName","productName","productPrice").
      agg(
        sum("orderItemSubtotal").as("revenue")
      ).orderBy('revenue.desc).
      withColumn("revenue",format_number('revenue,0))
    df.coalesce(1).write.parquet("/data/bigdata/data/retail_db/revenue_by_department")

  }


}
