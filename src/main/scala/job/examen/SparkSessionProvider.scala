package job.examen

import org.apache.spark.sql.SparkSession

object SparkSessionProvider {
  lazy val spark: SparkSession = SparkSession.builder
    .appName("Practica Examen") // Nombre identificativo para la aplicación.
    .master("local[2]") // Usa solo 2 núcleos para pruebas, suficiente para conjuntos de datos pequeños.
    .config("spark.driver.memory", "1g") // Ajusta la memoria a 1 GB para evitar problemas de recursos.
    .config("spark.ui.enabled", "false") // Desactiva la UI de Spark para ahorrar recursos.
    .getOrCreate()
}
