package com.sparkCovid

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.log4j.{Level, Logger}

object PacientesRecuperados
{
  def main(args: Array[String]): Unit =
  {
    // El metodo Logger solo permite mostrar en pantalla lineas de tipo ERROR o WARN si es el caso
    Logger.getLogger("org").setLevel(Level.ERROR)

    // declaramos las variables para el entorno de procesamiento spark sql
    val session = SparkSession.builder().appName("Pacientes Recuperados").master("local[*]").getOrCreate()
    // Cargamos el archivo .csv en un dataframe
    val datosCovid = session.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("input/datos-covid-col.csv")

    var df_covidSelect = datosCovid.
      select("Fecha de notificación", "Ciudad de ubicación", "atención", "Estado", "Edad", "Sexo", "Fecha diagnostico", "Fecha recuperado")
    //df_covidSelect.show(10)

    // Filtramos los pacientes que esten recuperados
    var df_covid_recuperados  = df_covidSelect.filter(df_covidSelect.col("atención").===("Recuperado"))
    println("Cantidad de pacientes recuperados: " + df_covid_recuperados.count())

    // Convertimos la columna de Fecha diagnostico de Timestamp a Date
    val df_InicioSintomas = df_covid_recuperados.withColumn("Fecha Inicio Sintomas", df_covid_recuperados("Fecha diagnostico").cast(DateType))

    // Convertimos la columna de Fecha Recuperado de Timestamp a Date
    val df_FinSintomas = df_InicioSintomas.withColumn("Fecha Recuperado Total", df_covid_recuperados("Fecha recuperado").cast(DateType))

    // Borramos las columnas repetidas del Dataframe
    var df_dropColumns = df_FinSintomas.drop("Fecha diagnostico", "Fecha recuperado", "Fecha de notificación")

    val df_covid_recuperados_dias = {
      df_dropColumns.select(col("Ciudad de ubicación"), col("atención"), col("Fecha Inicio Sintomas"), col("Fecha Recuperado Total"), datediff(to_date(col("Fecha Recuperado Total")), to_date(col("Fecha Inicio Sintomas"))).as("Dias Recuperacion"))
    }
    println("##################### Cantidad de dias de recuperacion por paciente #####################")
    df_covid_recuperados_dias.show(25)

    println("---------------  Pacientes recuperados por genero -----------------")
    // Normalizamos los valore del campo Sexo para que solo muestre F o M
    var df_GenerNormalizado = df_covidSelect.withColumn("Sexo", upper(col("Sexo")))
    var df_PacientespPorGenero = df_GenerNormalizado.groupBy("Sexo").count()
    df_PacientespPorGenero.show(5)






  }

}
