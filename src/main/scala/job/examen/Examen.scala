package job.examen

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Examen {

  // Sesión de Spark definida
  private val spark: SparkSession = SparkSession.builder()
    .appName("Practica Examen")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._ // IMPORTACIÓN CORRECTA PARA USAR $ y toDF

  def ejercicio1(estudiantes: DataFrame): DataFrame = {
    estudiantes.filter($"edad" > 20).orderBy($"edad".desc)
  }

  def ejercicio2(numeros: DataFrame): DataFrame = {
    val paridadUDF = udf((numero: Int) => if (numero % 2 == 0) "par" else "impar")
    numeros.withColumn("paridad", paridadUDF($"numero"))
  }

  def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {
    calificaciones.groupBy("id")
      .agg(avg("calificacion").as("promedio"))
      .join(estudiantes, Seq("id"))
      .select("nombre", "promedio")
  }

  def ejercicio4(palabras: List[String]): DataFrame = {
    palabras.toDF("palabra") // Convertir lista a DataFrame
      .groupBy("palabra")
      .count()
  }

  def ejercicio5(ventas: DataFrame): DataFrame = {
    ventas.withColumn("ingreso_total", $"cantidad" * $"precio_unitario")
      .select("id_producto", "ingreso_total")
  }
}
