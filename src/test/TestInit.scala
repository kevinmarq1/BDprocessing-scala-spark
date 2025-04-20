package job.examen.test

import org.apache.spark.sql.SparkSession

object TestInit {
  // Reutilizamos SparkSessionProvider pero le añadimos configuración específica para pruebas
  val spark: SparkSession = SparkSession.builder
    .appName("TestPractica")
    .master("local[*]") // Mantiene el cluster local para pruebas
    .config("spark.ui.enabled", "false") // Desactiva la interfaz web
    .getOrCreate()

  // Método para iniciar Spark en pruebas si es necesario
  def initialize(): SparkSession = {
    spark
  }
}
