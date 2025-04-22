package job.examen

import org.apache.spark.sql.SparkSession

object TestInit {
  val spark: SparkSession = SparkSession.builder
    .appName("Test de Practica")
    .master("local[*]") // Usa todos los núcleos disponibles para pruebas locales.
    .config("spark.driver.memory", "2g") // Asigna suficiente memoria.
    .config("spark.ui.enabled", "false") // Desactiva la UI de Spark para ahorrar recursos.
    .getOrCreate()

  def initialize(): SparkSession = spark
}
