package com.sparkCovid

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._

object FechasCovid
{
  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("Fechas Covid").master("local[*]").getOrCreate()

    // Cargamos el archivo .csv en un dataframe
    val datosCovid = session.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("input/datos-covid-col.csv")

    //datosCovid.show()
    /*
    ID de caso, Fecha de notificación, atención, Fecha diagnostico, Fecha recuperado
     */

    // Seleccionamos solo las columnas que necesitamos
    var df_FechasSeleccionadas = datosCovid.select("ID de caso", "atención", "Fecha diagnostico", "Fecha recuperado")
    df_FechasSeleccionadas.show(20)

    // Renombramos las columnas para que no tengan caracteres especiales
    var df_RenombrarColumnas = df_FechasSeleccionadas.withColumnRenamed("ID de caso", "ID_Caso")
      .withColumnRenamed("atención", "Atencion")
      .withColumnRenamed("Fecha diagnostico", "Fecha_Diagnostico")
      .withColumnRenamed("Fecha recuperado", "Fecha_Recuperado")
    df_RenombrarColumnas.show(30)

    //Convertimos las columnas de fecha que estan en Timestamp a Date
    var df_FechasDate = df_RenombrarColumnas
      .withColumn("Fecha_Diagnostico", df_RenombrarColumnas.col("Fecha_Diagnostico").cast(DateType))
      .withColumn("Fecha_Recuperado", df_RenombrarColumnas.col("Fecha_Recuperado").cast(DateType))
    df_FechasDate.show(10)

    // Extraemos el mes de cada una de las fechas
    var  df_ExtractMounth = df_FechasDate
      .withColumn("Mes_Diagnostico", date_format(col("Fecha_Diagnostico"), "MMMM"))
      .withColumn("Mes_Recuperado", date_format(col("Fecha_Recuperado"), "MMM"))
      .withColumn("Año_Recuperado", date_format(col("Fecha_Recuperado"), "YYYY"))
      .withColumn("Num_Mes_Rec", date_format(col("Fecha_Recuperado"), "MM"))
    df_ExtractMounth.show(50)

    // Para extraer el numero del mes, año, dia del mes, dia del año de la fecha
    df_FechasDate.select(
      col("Fecha_Diagnostico"),
      month(col("Fecha_Diagnostico")).as("NumMes"),
      year(col("Fecha_Diagnostico")).as("Año"),
      dayofmonth(col("Fecha_Diagnostico")).as("DiaMes"),
      dayofyear(col("Fecha_Diagnostico")).as("DiaAño")).show(30)


    // Concatenamos dos campos el año y el numero del mas para obtener el periodos
    df_ExtractMounth.withColumn("Periodo",
      concat(col("Año_Recuperado"),lit(""),col("Num_Mes_Rec")))
      .drop("Año_Recuperado")
      .drop("Num_Mes_Rec")
      .drop("Mes_Diagnostico")
      .drop("Mes_Recuperado")
      .show(35)
    
  }

}
