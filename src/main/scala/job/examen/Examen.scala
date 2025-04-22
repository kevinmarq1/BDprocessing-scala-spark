package job.examen

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import job.examen.SparkSessionProvider.spark
import spark.implicits._
import org.apache.spark.rdd.RDD


object Examen {
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

  val numeros: DataFrame = Seq(1, 2, 3, 4, 5, 6).toDF("numero")
  val palabras: List[String] = List("error", "warning", "error", "debug", "error", "info", "warning")
  val rutaArchivo = "src/resources/ventas.csv"
  val ventasDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(rutaArchivo)


  // **Ejercicio 1: Operaciones Básicas con DataFrames**
  def ejercicio1(estudiantes: DataFrame): DataFrame = {
    val filtroEdad = estudiantes.filter(col("edad") > 20)
    val nombresOrdenados = filtroEdad
      .select("nombre", "edad")
      .orderBy(col("edad").desc)
    nombresOrdenados
  }

  // **Ejercicio 2: UDF (User Defined Function)**
  def ejercicio2(numeros: DataFrame): DataFrame = {
    val esPar = udf((n: Int) => if (n % 2 == 0) "par" else "impar")
    numeros.withColumn("paridad", esPar($"numero"))
  }

  // **Ejercicio 3: Joins y Agregaciones**
  def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {
    val datosCompletos = estudiantes.join(calificaciones, "id") // Join por la columna "id"
    datosCompletos
      .groupBy("nombre") // Agrupa los datos por el nombre
      .agg(avg("calificacion").alias("promedio")) // Calcula el promedio de las calificaciones
  }

  // ejercicio 4 : RDDs y conteo de palabras
  def ejercicio4(palabras: List[String])(implicit spark: SparkSession): RDD[(String, Int)] = {
    val palabrasRDD: RDD[String] = spark.sparkContext.parallelize(palabras)
    palabrasRDD.map(palabra => (palabra, 1)).reduceByKey(_ + _)
  }

  // Ejercicio 5 : cargar csv y procesar los datos
  def ejercicio5(ventasDF: DataFrame): DataFrame = {
    ventasDF
      .withColumn("ingreso_total", col("cantidad") * col("precio_unitario"))
      .groupBy("id_producto")
      .agg(sum("ingreso_total").alias("ingreso_total"))
  }
}

