package job.examen

import org.apache.spark.sql.SparkSession

object SparkSessionProvider {

  // Inicialización de la sesión de Spark
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("Examen Practica")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
}

