package com.sparkCovid

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType}
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

    //println("---------------------    Cantidad de registros   -------------------")
    println("Casos confirmados hasta el periodo 20200807: " + datosCovid.count())

    println("---------------  Imprimimos la estructura del DataFrame   ---------------")
    // Imprimimos la estructura del DataFrame
    datosCovid.printSchema()

    println("---------------  Cantidad de por estado  ---------------")
    datosCovid.groupBy("Estado").count().show()

    println("---------------  Cantidad por atencion del paciente  ---------------")
    datosCovid.groupBy("atención").count().show()

    println("---------------  Ciudades con mayor numero de contagios ---------------")
    val covidCiudad = datosCovid.groupBy("Departamento o Distrito ").count()
    covidCiudad.orderBy(desc("count")).show()

    // Calculamos la genero y edad promedio de los pacientes
    println("-------------------- Genero y edad promedio de los contagiados -------------------")
    var df_edadProm = datosCovid.withColumn("Edad", col("Edad").cast(IntegerType))
    var df_GenEdad = df_edadProm.withColumn("Sexo", upper(col("Sexo")))

    df_GenEdad.groupBy("Sexo").avg("Edad").withColumnRenamed("Avg(Edad)", " Edad_Promedio").show()

    println("################## Grupos Etnicos ########################")

    // Ejemplo: Borrar los registros nulos por columnas seleccionadas
    // val noNullValues = load_DF.na.drop("all", Seq("x", "y"))
    var df_GrupoEtnico = datosCovid.na.drop("all", Seq("Nombre grupo etnico"))
    df_GrupoEtnico.show(30)
    println("La cantidad de registros de grupos etnicos son: " + df_GrupoEtnico.count())

    var df_GrupoEtnicoDepto = df_GrupoEtnico.groupBy("Departamento o Distrito ").count()

    // Cantidad de contagiados de grupos etnicos por departamento
    df_GrupoEtnicoDepto.orderBy(desc("count")).show()

    // Cantidad de contagiados de grupos etnicos en el Departamento del Quindio
    df_GrupoEtnicoDepto.filter(
      df_GrupoEtnicoDepto.col("Departamento o Distrito ") === "Quindío")
      .show()

    // Pendiente realizar un Case When o Swtch case en Scala
    /*
    Sintaxis:
    df.withColumn("new_column_name",
    when(<column_condition>, <value_when_true>).otherwise(<value_when_false>))

     */
    //Ejemplo
    // Scala:
    // Dataframe people

    /*people.select(when(people("gender") === "male", 0)
      .when(people("gender") === "female", 1)
      .otherwise(2))
    */

    val data = List(("James","","Smith","36636","M",60000),
      ("Michael","Rose","","40288","M",70000),
      ("Robert","","Williams","42114","",400000),
      ("Maria","Anne","Jones","39192","F",500000),
      ("Jen","Mary","Brown","","F",0))

    val cols = Seq("first_name","middle_name","last_name","dob","gender","salary")
    val df = session.createDataFrame(data).toDF(cols:_*)
    val df2 = df.withColumn("new_gender",
       when(col("gender") === "M","Male")
      .when(col("gender") === "F","Female")
      .otherwise("Unknown"))
    df2.show(5)

  }

}
