package net.atos.spark

import net.atos.spark.config.Constant
import net.atos.spark.service.LogicBase

object Main {

  def main(args:Array[String]): Unit ={

    val kpi = args(0)


    kpi match {
      case "revenue_by_dep" => LogicBase.revenueByDepartment
      case "revenue_by_prod"   => LogicBase.mostSoldItems
      case _  => println("unknown")
    }
    Constant.spark.stop()
  }

}
