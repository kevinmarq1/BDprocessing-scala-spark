package job.examen

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSessionProvider.spark

    println("\n--- Ejercicio 1 ---")
    Examen.ejercicio1()

    println("\n--- Ejercicio 2 ---")
    Examen.ejercicio2()

    println("\n--- Ejercicio 3 ---")
    Examen.ejercicio3()

    println("\n--- Ejercicio 4 ---")
    Examen.ejercicio4()

    println("\n--- Ejercicio 5 ---")
    Examen.ejercicio5()

  }
}
