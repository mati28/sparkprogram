package net.atos.spark.service

import net.atos.spark.config.Constant
import net.atos.spark.config.Utils

import org.apache.spark.sql.functions._

object LogicBase {

  val spark = Constant.spark

  import spark.implicits._

  val productsDF = Utils.getDF(Constant.PRODUCTS)
  val orderItemsDF = Utils.getDF(Constant.ORDERITEMS)
  val departmentsDF = Utils.getDF(Constant.DEPARTMENTS)
  val categoriesDF = Utils.getDF(Constant.CATEGORIES)

  def mostSoldItems() = {

    val mostSold = productsDF.join(orderItemsDF, 'productId <=> 'orderItemProductId).
      groupBy("productId", "productName").
      agg(
        count("productId").as("countItemsSold")
      ).sort('countItemsSold.desc).
      withColumn("countItemsSold", format_number('countItemsSold, 0))
    mostSold.coalesce(1).write.parquet("/data/bigdata/data/retail_db/countofsolditems_by_department")

  }

  def revenueByDepartment = {

    val df = departmentsDF.join(categoriesDF, 'departmentId <=> 'categoryDepartmentId).
      join(productsDF, 'categoryId <=> 'productCategoryId).
      join(orderItemsDF, 'productId <=> 'orderItemProductId).groupBy("departmentName", "productName", "productPrice").
      agg(
        sum("orderItemSubtotal").as("revenue")
      ).orderBy('revenue.desc).
      withColumn("revenue", format_number('revenue, 0))
    df.coalesce(1).write.parquet("/data/bigdata/data/retail_db/revenue_by_department")

  }


}
