package job.examen

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object Main {
  def main(args: Array[String]): Unit = {
    // Inicializar sesión de Spark
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Examen Main")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Datos para Ejercicio 1
    val estudiantes = Seq(
      (1, "Kevin", "Márquez", 25),
      (2, "Ana", "García", 22),
      (3, "Luis", "Fernández", 30),
      (4, "Sofía", "López", 19),
      (5, "Marta", "Pérez", 23)
    ).toDF("id", "nombre", "apellido", "edad")

    println("Ejercicio 1: Filtrar estudiantes mayores de 20 años y ordenarlos por edad")
    Examen.ejercicio1(estudiantes).show()

    // Datos para Ejercicio 2
    val numeros = Seq(1, 2, 3, 4, 5).toDF("numero")

    println("Ejercicio 2: Añadir la columna paridad")
    Examen.ejercicio2(numeros).show()

    // Datos para Ejercicio 3
    val calificaciones = Seq(
      (1, "Matemáticas", 8.5),
      (1, "Física", 9.0),
      (2, "Química", 5.5),
      (2, "Biología", 7.0),
      (3, "Literatura", 6.5),
      (3, "Historia", 7.5)
    ).toDF("id", "materia", "calificacion")

    println("Ejercicio 3: Calcular los promedios de calificaciones")
    Examen.ejercicio3(estudiantes, calificaciones).show()

    // Datos para Ejercicio 4
    val palabras = List("error", "warning", "error", "debug", "error", "info", "warning")

    println("Ejercicio 4: Contar palabras")
    Examen.ejercicio4(palabras).show()

    // Datos para Ejercicio 5
    val ventasDF = Seq(
      (101, 2, 230.0),
      (102, 3, 135.0),
      (103, 4, 70.0)
    ).toDF("id_producto", "cantidad", "precio_unitario")

    println("Ejercicio 5: Calcular el ingreso total por producto")
    Examen.ejercicio5(ventasDF).show()
  }
}
