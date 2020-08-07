package com.sparkCovid

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

object DatosCovid
{

  def main(args: Array[String]): Unit =
  {
    // El metodo Logger solo permite mostrar en pantalla lineas de tipo ERROR o WARN si es el caso
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("Datos_Covid").setMaster("local[*]")
    val sc = new SparkContext(conf)

    println("Hello Scala Program")
    println("Hello My Name is Juan David")


  }


}
