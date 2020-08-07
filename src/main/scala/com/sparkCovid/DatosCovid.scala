package com.sparkCovid

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}


object DatosCovid
{

  def main(args: Array[String]): Unit =
  {
    // El metodo Logger solo permite mostrar en pantalla lineas de tipo ERROR o WARN si es el caso
    Logger.getLogger("org").setLevel(Level.ERROR)
    // declaramos las variables para el entorno de procesamiento spark sql
    val session = SparkSession.builder().appName("Datos Covid").master("local[*]").getOrCreate()

    // Fuente: https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia/gt2j-8ykr/data

    // Cargamos el archivo .csv en un dataframe
    val datosCovid = session.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("input/datos-covid-col.csv")

    // Imprimimos el dataframe
    datosCovid.show()

    println("---------------------    Cantidad de registros   -------------------")
    println(datosCovid.count())

    println("---------------  Imprimimos la estructura del DataFrame   ---------------")
    // Imprimimos la estructura del DataFrame
    datosCovid.printSchema()

  }

}
