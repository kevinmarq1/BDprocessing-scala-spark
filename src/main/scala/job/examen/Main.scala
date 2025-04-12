package job.examen

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSessionProvider.spark

    println("\n--- Ejercicio 1 ---")
    examen.ejercicio1()

    println("\n--- Ejercicio 2 ---")
    examen.ejercicio2()

    println("\n--- Ejercicio 3 ---")
    examen.ejercicio3()
  }
}
