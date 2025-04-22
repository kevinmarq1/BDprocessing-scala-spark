package job.examen

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSessionProvider.spark

    println("\n--- Ejercicio 1 ---")
    Examen.ejercicio1(Examen.estudiantes).show()

    println("\n--- Ejercicio 2 ---")
    Examen.ejercicio2(Examen.numeros).show()

    println("\n--- Ejercicio 3 ---")
    Examen.ejercicio3(Examen.estudiantes, Examen.calificaciones).show()

    println("\n--- Ejercicio 4 ---")
    Examen.ejercicio4(Examen.palabras).collect().foreach { case (palabra, frecuencia) =>
      println(s"$palabra: $frecuencia")
    }

    println("\n--- Ejercicio 5 ---")
    Examen.ejercicio5(Examen.ventasDF).show()
  }
}
