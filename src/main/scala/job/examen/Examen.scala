package job.examen

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import job.examen.SparkSessionProvider.spark
import spark.implicits._

object examen {

  // **Definición de DataFrames Compartidos**
  val estudiantes: DataFrame = Seq(
    (1, "Kevin", "Márquez", 25),
    (2, "Ana", "García", 22),
    (3, "Luis", "Fernández", 30),
    (4, "Sofía", "López", 19),
    (5, "Marta", "Pérez", 23)
  ).toDF("id", "nombre", "apellido", "edad")

  val calificaciones: DataFrame = Seq(
    (1, "Matemáticas", 8.5, "Sí"),
    (1, "Física", 9.0, "Sí"),
    (2, "Química", 5.5, "No"),
    (2, "Biología", 7.0, "Sí"),
    (3, "Literatura", 6.5, "No"),
    (3, "Historia", 7.5, "Sí"),
    (4, "Arte", 8.0, "Sí"),
    (4, "Informática", 9.5, "Sí"),
    (5, "Geografía", 4.0, "No"),
    (5, "Educación Física", 6.0, "No")
  ).toDF("id", "materia", "calificacion", "apto")

  // **Ejercicio 1: Operaciones Básicas con DataFrames**
  def ejercicio1()(implicit spark: SparkSession): DataFrame = {
    val filtroEdad = estudiantes.filter(col("edad") > 20)
    val nombresOrdenados = filtroEdad
      .select("nombre", "edad")
      .orderBy(col("edad").desc)
    nombresOrdenados.show()

    nombresOrdenados
  }

  // **Ejercicio 2: UDF (User Defined Function)**
  def ejercicio2()(implicit spark: SparkSession): DataFrame = {
    val numeros = Seq(1, 2, 3, 4, 5, 6).toDF("numero")
    val esPar = udf((n: Int) => if (n % 2 == 0) "par" else "impar")
    val resultado = numeros.withColumn("paridad", esPar($"numero"))
    resultado.show()

    resultado
  }

  // **Ejercicio 3: Joins y Agregaciones**
  def ejercicio3()(implicit spark: SparkSession): DataFrame = {
    val datosCompletos = estudiantes.join(calificaciones, "id")
    val resultado = datosCompletos
      .groupBy("nombre")
      .agg(avg("calificacion").alias("promedio"))
    resultado.show()

    resultado
  }
}
