package job.examen

import org.apache.spark.sql.SparkSession

object SparkSessionProvider {
  lazy val spark: SparkSession = SparkSession.builder
    .appName("Practica Examen")
    .master("local[*]") // Usa la configuración predeterminada para recursos locales
    .getOrCreate()
}

