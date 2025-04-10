package job.examen

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder
      .appName("Practica")
      .master("local")
      .getOrCreate()

    // Llamada al ejercicio 1
    examen.ejercicio1()
    // Llamada al ejercicio 2
    examen.ejercicio2()
  }
}
