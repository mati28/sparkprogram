package net.atos.spark

import net.atos.spark.config.Constant
import net.atos.spark.service.LogicBase

object SparkWithConfFile {


  def main(args:Array[String]): Unit ={

    val kpi = args(0)


    kpi match {
      case "revenue" => LogicBase.revenueByDepartment
      case "count"   => LogicBase.mostSoldItems
      case _  => println("unknown")
    }
    Constant.spark.stop()
  }

}
