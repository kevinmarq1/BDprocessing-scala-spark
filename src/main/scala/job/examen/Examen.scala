package job.examen

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object examen {

  def ejercicio1()(implicit spark: SparkSession): DataFrame = {
    // Lista de estudiantes
    val estudiantes = Seq(
      ("Kevin", 20, 9),
      ("Laura", 22, 7),
      ("Carlos", 21, 8),
      ("Ana", 23, 10)
    )

    // Crear DataFrame
    val dfEstudiantes = spark.createDataFrame(estudiantes).toDF("nombre", "edad", "calificacion")

    // Muestra el esquema del DataFrame
    dfEstudiantes.printSchema()

    // Filtra estudiantes con calificación > 8
    val filtroCalificacion = dfEstudiantes.filter(col("calificacion") > 8)
    filtroCalificacion.show()

    // Ordena nombres por calificación descendente
    val nombresOrdenados = filtroCalificacion.select("nombre").orderBy(col("calificacion").desc)
    nombresOrdenados.show()

    nombresOrdenados
  }

  def ejercicio2()(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    import org.apache.spark.sql.functions.udf

    // Crear un DataFrame con números
    val numeros = Seq(1, 2, 3, 4, 5, 6).toDF("numero")

    // Definir la UDF para determinar paridad
    val esPar = udf((n: Int) => if (n % 2 == 0) "par" else "impar")

    // Aplicar la UDF al DataFrame
    val resultado = numeros.withColumn("paridad", esPar($"numero"))

    // Mostrar los resultados
    resultado.show()

    resultado // Devolver el DataFrame resultante
  }

}

  // Puedes añadir más ejercicios según sea necesario

