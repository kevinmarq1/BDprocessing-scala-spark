package job.examen

import org.apache.spark.sql.SparkSession

object SparkSessionProvider {
  val spark: SparkSession = SparkSession.builder
    .appName("Practica Examen")
    .master("local[*]") // Usa todos los núcleos disponibles
    .config("spark.driver.memory", "1g") // Aumenta la memoria del driver a 1 GB
    .config("spark.ui.enabled", "false") // Desactiva la UI para pruebas
    .getOrCreate()
}
