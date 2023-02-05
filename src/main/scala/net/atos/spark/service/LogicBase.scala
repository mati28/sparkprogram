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

    val df = productsDF.join(orderItemsDF, 'productId <=> 'orderItemProductId).
      groupBy("productId", "productName").
      agg(
        count("productId").as("countItemsSold"),
        sum("orderItemSubtotal").as("ca")
      ).orderBy('countItemsSold.desc,'ca.desc).
      withColumn("countItemsSold", format_number('countItemsSold, 0)).
      withColumn("ca", format_number('ca,4))

    val path: String = "/data/bigdata/data/countofsolditems_by_department"
    Utils.saveDF(df, path)

  }

  def revenueByDepartment = {

    val df = departmentsDF.join(categoriesDF, 'departmentId <=> 'categoryDepartmentId).
      join(productsDF, 'categoryId <=> 'productCategoryId).
      join(orderItemsDF, 'productId <=> 'orderItemProductId).groupBy("departmentName", "productName", "productPrice").
      agg(
        sum("orderItemSubtotal").as("revenue")
      ).orderBy('revenue.desc).
      withColumn("revenue", format_number('revenue, 0))
    //Save revenue to this location
    val path: String = "/data/bigdata/data/revenue_by_department"
    Utils.saveDF(df, path)

  }


}
